// Package engine 提供通用 Checkpoint 编排主循环：Load → 对齐 → Execute → Save。
//
// 引擎只需声明阶段词汇表（PhaseSet）并实现「执行一步业务逻辑」（Execute），
// 框架负责初始化、恢复编排、快照重试与原子持久化。加阶段 = 声明数据，
// 不是改框架。
//
// 失败语义（ADR-1）：唯一落盘失败路径是优雅失败——引擎在 Execute 内置
// PhaseFailed 并返回 nil，框架 Save 后停止。Execute 返回非 nil error 表示
// 可重试/可恢复：重试耗尽后 Run 返回 error 不落盘，任务停留于最后已提交
// 阶段，下次 Run 从该阶段恢复。框架不兜底写终态。
//
// 并发边界：单任务单执行器——同一 taskID 同时至多一个 Run 在推进；
// 引擎实例默认每任务独立，声明并发安全者除外（设计 §2.4）。
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jninng/waypoint/state"
)

// PhaseSet 是引擎声明的阶段词汇表。
type PhaseSet struct {
	All         []state.Phase // 全部合法阶段
	Initial     state.Phase   // 新任务初始阶段；零值 = PhasePending
	Recoverable []state.Phase // 恢复时需要对齐（Align）的阶段
	Terminal    []state.Phase // 终态；Run 达到后停止循环
}

// IsTerminal 判定 p 是否终态。框架与调用方共用同一判定来源。
func (ps PhaseSet) IsTerminal(p state.Phase) bool { return ps.contains(ps.Terminal, p) }

// IsRecoverable 判定 p 是否恢复时需要对齐的阶段。
func (ps PhaseSet) IsRecoverable(p state.Phase) bool { return ps.contains(ps.Recoverable, p) }

// initial 返回生效的初始阶段（零值视作 PhasePending）。
func (ps PhaseSet) initial() state.Phase {
	if ps.Initial == "" {
		return state.PhasePending
	}
	return ps.Initial
}

func (ps PhaseSet) contains(set []state.Phase, p state.Phase) bool {
	for _, x := range set {
		if x == p {
			return true
		}
	}
	return false
}

// validate 执行 Run 进入循环前的强制校验（设计 §5.2 校验清单）。
func (ps PhaseSet) validate() error {
	if len(ps.All) == 0 {
		return fmt.Errorf("engine: PhaseSet.All must not be empty")
	}
	init := ps.initial()
	if !ps.contains(ps.All, init) {
		return fmt.Errorf("engine: PhaseSet.Initial %q must be in All", init)
	}
	if ps.IsTerminal(init) {
		return fmt.Errorf("engine: PhaseSet.Initial %q must not be Terminal", init)
	}
	for _, p := range ps.Recoverable {
		if !ps.contains(ps.All, p) {
			return fmt.Errorf("engine: Recoverable phase %q must be in All", p)
		}
	}
	for _, p := range ps.Terminal {
		if !ps.contains(ps.All, p) {
			return fmt.Errorf("engine: Terminal phase %q must be in All", p)
		}
	}
	// PhaseFailed ∈ All 时必须 ∈ Terminal，否则引擎置 failed 后循环不结束。
	if ps.contains(ps.All, state.PhaseFailed) && !ps.IsTerminal(state.PhaseFailed) {
		return fmt.Errorf("engine: PhaseFailed is in All but not Terminal")
	}
	return nil
}

// DefaultPhaseSet 返回默认协议阶段：pending → running → completed / failed。
func DefaultPhaseSet() PhaseSet {
	return PhaseSet{
		All:         []state.Phase{state.PhasePending, state.PhaseRunning, state.PhaseCompleted, state.PhaseFailed},
		Initial:     state.PhasePending,
		Recoverable: []state.Phase{state.PhaseRunning},
		Terminal:    []state.Phase{state.PhaseCompleted, state.PhaseFailed},
	}
}

// Engine 是异步任务的执行契约。
type Engine interface {
	TaskType() string
	Phases() PhaseSet
	// Execute 执行一步业务逻辑并返回新 LSN。
	//
	// 时序契约：先产生物理副作用，框架后保存 checkpoint；崩溃时物理系统可能
	// 领先于 checkpoint，恢复路径由 Aligner 对齐。
	//
	// 重入契约：Execute 的副作用必须可安全重放（幂等）或可对齐——已在
	// Execute 产生但未 checkpoint 的副作用，重试/恢复时重放不得累积。
	//
	// 优雅失败：引擎在 Execute 内置 PhaseFailed 并返回 nil，失败原因写入
	// Message——这是唯一落盘的失败路径（ADR-1）。
	Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error)
}

// Aligner 是可选接口：把物理系统对齐到 targetLSN（恢复路径调用）。
// 无此能力的引擎不实现，不留空方法。
//
// Align 不得依赖 Stater 内容——需要分块/偏移元数据才能对齐的引擎，
// 必须把该信息可逆编码进 LSN。
type Aligner interface {
	Align(ctx context.Context, targetLSN state.LSN) error
}

// Progressor 是可选接口：返回 0-100 进度。无 Sizer 时可为 0，不得伪造。
type Progressor interface {
	Progress(s state.BaseTaskState) int
}

// runConfig 是 RunOption 的作用目标。
type runConfig struct {
	maxRetries int
	delay      time.Duration
}

// RunOption 配置 Run 的行为。
type RunOption func(*runConfig)

// WithRetry 配置 Execute 的进程内重试：失败后回滚到本步快照再重试，
// 最多额外重试 maxRetries 次，重试间隔 delay（可为 0）。
// 负值参数构造时 panic。仅作用于 Execute；Load/Save/Align 错误不重试。
func WithRetry(maxRetries int, delay time.Duration) RunOption {
	if maxRetries < 0 {
		panic(fmt.Sprintf("engine: WithRetry maxRetries must be >= 0, got %d", maxRetries))
	}
	if delay < 0 {
		panic(fmt.Sprintf("engine: WithRetry delay must be >= 0, got %s", delay))
	}
	return func(c *runConfig) {
		c.maxRetries = maxRetries
		c.delay = delay
	}
}

// Run 运行任务直到终态，成功返回最终已保存状态（失败返回 nil, err）——
// 桥接与调用方无需二次 Load。
//
// 主循环语义（设计 §5.2）：
//  1. 校验 Phases()。
//  2. Load：nil → 以 Initial 初始化；否则反序列化并校验 Phase ∈ All
//     且 TaskType 匹配（防止用错引擎恢复旧状态）。
//  3. 恢复：Phase ∈ Recoverable 且 ∉ Terminal 且引擎实现 Aligner →
//     Align(ctx, CheckpointLSN)；此步先于任何 Stater 还原。
//  4. 循环直到 Phase ∈ Terminal：每次 Execute 前深快照 BaseTaskState，
//     失败回滚到快照再按 WithRetry 重试；成功后校验 Phase ∈ All、
//     更新 CheckpointLSN 与 Progress，无条件 Save。
//  5. ctx 取消：返回 ctx.Err()，不落盘。
func Run(ctx context.Context, repo state.StateRepository, eng Engine, taskID string, opts ...RunOption) (*state.BaseTaskState, error) {
	cfg := runConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	ps := eng.Phases()
	if err := ps.validate(); err != nil {
		return nil, err
	}

	data, err := repo.Load(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("engine: load %s: %w", taskID, err)
	}
	var s *state.BaseTaskState
	if data == nil {
		s = &state.BaseTaskState{
			TaskID:   taskID,
			TaskType: eng.TaskType(),
			Phase:    ps.initial(),
		}
	} else {
		s = &state.BaseTaskState{}
		if err := json.Unmarshal(data, s); err != nil {
			return nil, fmt.Errorf("engine: decode state %s: %w", taskID, err)
		}
		if s.TaskType != eng.TaskType() {
			return nil, fmt.Errorf("engine: state task type %q does not match engine task type %q", s.TaskType, eng.TaskType())
		}
		if !ps.contains(ps.All, s.Phase) {
			return nil, fmt.Errorf("engine: state phase %q is not in PhaseSet.All", s.Phase)
		}
	}

	// Recoverable ∩ Terminal ≠ ∅ 时终态优先：终态任务不触发 Align。
	if ps.IsRecoverable(s.Phase) && !ps.IsTerminal(s.Phase) {
		if al, ok := eng.(Aligner); ok {
			if err := al.Align(ctx, s.CheckpointLSN); err != nil {
				return nil, fmt.Errorf("engine: align %s to lsn %d: %w", taskID, s.CheckpointLSN, err)
			}
		}
	}

	for !ps.IsTerminal(s.Phase) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lsn, err := executeWithRetry(ctx, eng, s, &cfg)
		if err != nil {
			return nil, err
		}
		if !ps.contains(ps.All, s.Phase) {
			return nil, fmt.Errorf("engine: execute set phase %q which is not in PhaseSet.All", s.Phase)
		}
		s.CheckpointLSN = lsn
		if pr, ok := eng.(Progressor); ok {
			s.Progress = pr.Progress(*s)
		}
		out, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("engine: encode state %s: %w", taskID, err)
		}
		if err := repo.Save(ctx, taskID, out); err != nil {
			return nil, fmt.Errorf("engine: save %s: %w", taskID, err)
		}
	}
	return s, nil
}

// executeWithRetry 执行一步并处理重试（ADR-5：重试以快照为起点）。
func executeWithRetry(ctx context.Context, eng Engine, s *state.BaseTaskState, cfg *runConfig) (state.LSN, error) {
	snap := cloneState(s)
	for attempt := 0; ; attempt++ {
		lsn, err := eng.Execute(ctx, s)
		if err == nil {
			return lsn, nil
		}
		if cerr := ctx.Err(); cerr != nil {
			return 0, cerr
		}
		// 回滚到快照：失败调用的部分状态修改不得泄漏进重试。
		*s = *cloneState(snap)
		if attempt >= cfg.maxRetries {
			return 0, err
		}
		if serr := sleepCtx(ctx, cfg.delay); serr != nil {
			return 0, serr
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// cloneState 深拷贝整个 BaseTaskState（含 Payload 字节）。
func cloneState(s *state.BaseTaskState) *state.BaseTaskState {
	c := *s
	if s.Payload != nil {
		c.Payload = append(json.RawMessage(nil), s.Payload...)
	}
	return &c
}
