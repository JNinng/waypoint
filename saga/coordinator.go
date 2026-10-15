package saga

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Logger 是注入式日志接口（*log/slog.Logger 满足之）。默认静默，
// 库不向标准输出打印。
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// SagaCoordinator 按 Saga 定义推进 SagaState：Run 新建执行，Resume 依据
// 持久化状态继续。并发边界：同一 sagaID 同时至多一个协调器实例。
type SagaCoordinator struct {
	store  SagaStore
	logger Logger
}

// SagaCoordinatorOption 配置 SagaCoordinator。
type SagaCoordinatorOption func(*SagaCoordinator)

// WithSagaLogger 注入日志（默认静默）。
func WithSagaLogger(l Logger) SagaCoordinatorOption {
	return func(c *SagaCoordinator) { c.logger = l }
}

// NewSagaCoordinator 创建协调器。
func NewSagaCoordinator(store SagaStore, opts ...SagaCoordinatorOption) *SagaCoordinator {
	c := &SagaCoordinator{store: store, logger: nopLogger{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run 新建执行（状态已存在则报错），推进到终态后返回最终状态。
func (c *SagaCoordinator) Run(ctx context.Context, saga *Saga, sagaID string) (*SagaState, error) {
	existing, err := c.store.Load(ctx, sagaID)
	if err != nil {
		return nil, fmt.Errorf("saga: load %s: %w", sagaID, err)
	}
	if existing != nil {
		return nil, fmt.Errorf("saga: saga %s already exists", sagaID)
	}
	st := &SagaState{
		SagaID:          sagaID,
		SagaName:        saga.Name,
		Status:          StatusPending,
		CurrentStep:     0,
		StepStatuses:    make([]StepStatus, len(saga.Steps)),
		StepFingerprint: stepFingerprint(saga),
		ActionCtx:       ActionCtx{},
		CreatedAt:       time.Now().UTC(),
	}
	for i := range st.StepStatuses {
		st.StepStatuses[i] = StepPending
	}
	touch(st)
	if err := c.store.Save(ctx, cloneSagaState(st)); err != nil {
		return nil, fmt.Errorf("saga: save %s: %w", sagaID, err)
	}
	return c.advance(ctx, saga, st)
}

// Resume 依据持久化 SagaState 继续；步骤定义指纹不一致拒绝恢复；
// 已终态的状态原样返回（幂等 Resume）。
func (c *SagaCoordinator) Resume(ctx context.Context, saga *Saga, sagaID string) (*SagaState, error) {
	st, err := c.store.Load(ctx, sagaID)
	if err != nil {
		return nil, fmt.Errorf("saga: load %s: %w", sagaID, err)
	}
	if st == nil {
		return nil, fmt.Errorf("saga: saga %s not found", sagaID)
	}
	if fp := stepFingerprint(saga); fp != st.StepFingerprint {
		return nil, fmt.Errorf("saga: step fingerprint mismatch for %s (stored %s, definition %s): refusing to resume", sagaID, st.StepFingerprint, fp)
	}
	return c.advance(ctx, saga, st)
}

// advance 依据当前状态推进到终态。
func (c *SagaCoordinator) advance(ctx context.Context, saga *Saga, st *SagaState) (*SagaState, error) {
	switch st.Status {
	case StatusPending:
		st.Status = StatusRunning
		touch(st)
		if err := c.save(ctx, st); err != nil {
			return nil, err
		}
		return c.forward(ctx, saga, st)
	case StatusRunning:
		return c.forward(ctx, saga, st)
	case StatusCompensating:
		return c.compensateAll(ctx, saga, st)
	case StatusCompleted, StatusCompensated, StatusFailed:
		return st, nil
	default:
		return nil, fmt.Errorf("saga: unknown saga status %q", st.Status)
	}
}

// forward 正向推进：按序执行 Action，跳过已 completed 的步骤；
// 重放 StepRunning 步骤（重入契约）。
func (c *SagaCoordinator) forward(ctx context.Context, saga *Saga, st *SagaState) (*SagaState, error) {
	for i := 0; i < len(saga.Steps); i++ {
		switch st.StepStatuses[i] {
		case StepCompleted:
			continue
		case StepPending, StepRunning:
		default:
			return nil, fmt.Errorf("saga: inconsistent step status %q at index %d while saga is running", st.StepStatuses[i], i)
		}
		if err := ctx.Err(); err != nil {
			return c.cancelAndReturn(ctx, st, err)
		}
		st.StepStatuses[i] = StepRunning
		st.CurrentStep = i
		touch(st)
		if err := c.save(ctx, st); err != nil {
			return nil, err
		}
		err := c.execWithRetry(ctx, saga, st, i, saga.Steps[i].Action)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return c.cancelAndReturn(ctx, st, cerr)
			}
			st.StepStatuses[i] = StepFailed
			recordStepError(st, i, err.Error())
			st.Status = StatusCompensating
			st.CurrentStep = i - 1
			touch(st)
			if serr := c.save(ctx, st); serr != nil {
				return nil, serr
			}
			c.logger.Warn("saga step failed, compensating", "saga", st.SagaID, "step", saga.Steps[i].Name, "error", err)
			return c.compensateAll(ctx, saga, st)
		}
		st.StepStatuses[i] = StepCompleted
		st.CurrentStep = i + 1
		touch(st)
		if err := c.save(ctx, st); err != nil {
			return nil, err
		}
	}
	st.Status = StatusCompleted
	touch(st)
	if err := c.save(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

// compensateAll 逆序补偿：只补偿 StepCompleted 的步骤，跳过已
// compensated 的；nil Compensation 视为幂等步骤直接标记 compensated；
// 单个补偿失败记入 StepErrors 后继续（不阻断）。
func (c *SagaCoordinator) compensateAll(ctx context.Context, saga *Saga, st *SagaState) (*SagaState, error) {
	compFailed := false
	for j := len(saga.Steps) - 1; j >= 0; j-- {
		if st.StepStatuses[j] != StepCompleted {
			continue
		}
		if err := ctx.Err(); err != nil {
			return c.cancelAndReturn(ctx, st, err)
		}
		step := saga.Steps[j]
		if step.Compensation == nil {
			st.StepStatuses[j] = StepCompensated
			st.CurrentStep = j - 1
			touch(st)
			if err := c.save(ctx, st); err != nil {
				return nil, err
			}
			continue
		}
		st.CurrentStep = j
		touch(st)
		if err := c.save(ctx, st); err != nil {
			return nil, err
		}
		err := c.execWithRetry(ctx, saga, st, j, step.Compensation)
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return c.cancelAndReturn(ctx, st, cerr)
			}
			recordStepError(st, j, err.Error())
			compFailed = true
			c.logger.Error("saga compensation failed", "saga", st.SagaID, "step", step.Name, "error", err)
			continue
		}
		st.StepStatuses[j] = StepCompensated
		st.CurrentStep = j - 1
		touch(st)
		if err := c.save(ctx, st); err != nil {
			return nil, err
		}
	}
	if compFailed {
		st.Status = StatusFailed
	} else {
		st.Status = StatusCompensated
	}
	touch(st)
	if err := c.save(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

// execWithRetry 执行一次 Action 或 Compensation，施加重试链与超时；
// ActionCtx 来自当前 SagaState（步骤间共享）。
func (c *SagaCoordinator) execWithRetry(ctx context.Context, saga *Saga, st *SagaState, i int,
	fn func(context.Context, ActionCtx) error) error {

	step := saga.Steps[i]
	attempts := resolveAttempts(step, saga)
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		actx := ctx
		var cancel context.CancelFunc
		if step.Timeout > 0 {
			actx, cancel = context.WithTimeout(ctx, step.Timeout)
		}
		err := fn(actx, st.ActionCtx)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			return nil
		}
		lastErr = err
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if attempt < attempts-1 {
			c.logger.Warn("saga step attempt failed, retrying",
				"step", step.Name, "attempt", attempt+1, "of", attempts, "error", err)
		}
	}
	return fmt.Errorf("step %s failed after %d attempt(s): %w", step.Name, attempts, lastErr)
}

// cancelAndReturn 以 context.WithoutCancel 保存当前状态后返回 ctx.Err()；
// Save 错误与取消错误合并上抛，不吞。
func (c *SagaCoordinator) cancelAndReturn(ctx context.Context, st *SagaState, cause error) (*SagaState, error) {
	touch(st)
	if err := c.store.Save(context.WithoutCancel(ctx), cloneSagaState(st)); err != nil {
		return nil, errors.Join(cause, fmt.Errorf("saga: save %s on cancel: %w", st.SagaID, err))
	}
	return nil, cause
}

func (c *SagaCoordinator) save(ctx context.Context, st *SagaState) error {
	if err := c.store.Save(ctx, cloneSagaState(st)); err != nil {
		return fmt.Errorf("saga: save %s: %w", st.SagaID, err)
	}
	return nil
}

func touch(st *SagaState) { st.UpdatedAt = time.Now().UTC() }
