package saga

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// ctxSpyStore 记录 Save 时的 ctx 状态（验证取消路径用 WithoutCancel）。
type ctxSpyStore struct {
	*InMemorySagaStore
	mu        sync.Mutex
	cancelled []bool // 每次 Save 时 ctx.Err() != nil
}

func newSpyStore() *ctxSpyStore {
	return &ctxSpyStore{InMemorySagaStore: NewInMemorySagaStore()}
}

func (s *ctxSpyStore) Save(ctx context.Context, st *SagaState) error {
	s.mu.Lock()
	s.cancelled = append(s.cancelled, ctx.Err() != nil)
	s.mu.Unlock()
	return s.InMemorySagaStore.Save(ctx, st)
}

func ok(name string) SagaStep {
	return SagaStep{
		Name: name,
		Action: func(ctx context.Context, ac ActionCtx) error {
			ac[name] = true
			return nil
		},
		Compensation: func(ctx context.Context, ac ActionCtx) error {
			ac[name+"-undone"] = true
			return nil
		},
	}
}

func fail(name string) SagaStep {
	return SagaStep{
		Name: name,
		Action: func(ctx context.Context, ac ActionCtx) error {
			return errors.New("step " + name + " exploded")
		},
	}
}

func TestSagaHappyPath(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{
		Name:  "order",
		Steps: []SagaStep{ok("reserve"), ok("charge"), ok("ship")},
	}
	st, err := c.Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompleted {
		t.Fatalf("status = %q", st.Status)
	}
	if st.CurrentStep != 3 {
		t.Fatalf("CurrentStep = %d, want 3（第一个未完成步骤索引）", st.CurrentStep)
	}
	for i, ss := range st.StepStatuses {
		if ss != StepCompleted {
			t.Fatalf("step %d status = %q", i, ss)
		}
	}
	// ActionCtx 传递。
	if st.ActionCtx["ship"] != true {
		t.Fatalf("ActionCtx = %v", st.ActionCtx)
	}
	// 持久化一致。
	persisted, _ := store.Load(context.Background(), "s1")
	if persisted.Status != StatusCompleted || persisted.StepFingerprint != st.StepFingerprint {
		t.Fatalf("persisted = %+v", persisted)
	}
}

// 终态三分之 compensated：正向失败、补偿全部成功（已回滚）。
func TestSagaCompensated(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{
		Name:  "order",
		Steps: []SagaStep{ok("reserve"), ok("charge"), fail("ship")},
	}
	st, err := c.Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompensated {
		t.Fatalf("status = %q, want compensated", st.Status)
	}
	if st.StepStatuses[0] != StepCompensated || st.StepStatuses[1] != StepCompensated {
		t.Fatalf("steps = %v", st.StepStatuses)
	}
	if st.StepStatuses[2] != StepFailed {
		t.Fatalf("失败步 status = %q", st.StepStatuses[2])
	}
	// 原始失败原因持久保留。
	if !strings.Contains(st.StepErrors[2], "ship exploded") {
		t.Fatalf("StepErrors = %v", st.StepErrors)
	}
}

// 终态三分之 failed：任一补偿失败——需人工介入；补偿失败不阻断其余补偿。
func TestSagaFailedWhenCompensationFails(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	compRuns := 0
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{
			ok("reserve"),
			{
				Name:   "charge",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					compRuns++
					return errors.New("refund gateway down")
				},
			},
			{
				Name:   "notify",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					compRuns++
					return nil
				},
			},
			fail("ship"),
		},
	}
	st, err := c.Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusFailed {
		t.Fatalf("status = %q, want failed", st.Status)
	}
	if compRuns != 2 {
		t.Fatalf("单个补偿失败不得阻断其余补偿, runs = %d", compRuns)
	}
	// 补偿失败原因与原始失败原因都持久保留。
	if !strings.Contains(st.StepErrors[1], "refund gateway down") {
		t.Fatalf("补偿失败原因未保留: %v", st.StepErrors)
	}
	if !strings.Contains(st.StepErrors[3], "ship exploded") {
		t.Fatalf("原始失败原因未保留: %v", st.StepErrors)
	}
}

// 补偿可重试（同一重试链）：第二次尝试成功。
func TestSagaCompensationRetried(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	attempts := 0
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{
			{
				Name:   "reserve",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					attempts++
					if attempts == 1 {
						return errors.New("transient")
					}
					return nil
				},
				MaxRetries: 3,
			},
			fail("charge"),
		},
	}
	st, err := c.Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompensated {
		t.Fatalf("status = %q, want compensated（补偿重试后成功）", st.Status)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

// 补偿可超时（同一 Timeout 施加方式）。
func TestSagaCompensationTimeout(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{
			{
				Name:   "reserve",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					<-ctx.Done() // 挂死，等待超时截断
					return ctx.Err()
				},
				MaxRetries: 2,
				Timeout:    20 * time.Millisecond,
			},
			fail("charge"),
		},
	}
	st, err := c.Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusFailed {
		t.Fatalf("status = %q, want failed（补偿挂死被超时截断）", st.Status)
	}
}

// Action 重试链：step.MaxRetries → saga.DefaultMaxRetries → 1（下限 1）。
func TestSagaActionRetryChain(t *testing.T) {
	cases := []struct {
		name      string
		stepMax   int
		sagaMax   int
		wantTries int
	}{
		{"step-level", 3, 5, 3},
		{"saga-default", 0, 2, 2},
		{"floor-one", 0, 0, 1},
		{"negative-floored", -2, 4, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tries := 0
			sg := &Saga{
				Name: "order",
				Steps: []SagaStep{{
					Name: "a",
					Action: func(ctx context.Context, ac ActionCtx) error {
						tries++
						return errors.New("boom")
					},
					MaxRetries: tc.stepMax,
				}},
				DefaultMaxRetries: tc.sagaMax,
			}
			st, err := NewSagaCoordinator(NewInMemorySagaStore()).Run(context.Background(), sg, "s1")
			if err != nil {
				t.Fatal(err)
			}
			if st.Status != StatusCompensated {
				t.Fatalf("status = %q", st.Status)
			}
			if tries != tc.wantTries {
				t.Fatalf("tries = %d, want %d", tries, tc.wantTries)
			}
		})
	}
}

// Action 超时：单次尝试限时，重试链耗尽后进入补偿。
func TestSagaActionTimeout(t *testing.T) {
	tries := 0
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{{
			Name: "hang",
			Action: func(ctx context.Context, ac ActionCtx) error {
				tries++
				<-ctx.Done()
				return ctx.Err()
			},
			MaxRetries: 2,
			Timeout:    20 * time.Millisecond,
		}},
	}
	st, err := NewSagaCoordinator(NewInMemorySagaStore()).Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if tries != 2 {
		t.Fatalf("tries = %d", tries)
	}
	if st.Status != StatusCompensated {
		t.Fatalf("status = %q", st.Status)
	}
}

// ADR-4：指纹不一致拒绝恢复。
func TestSagaFingerprintMismatch(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{Name: "order", Steps: []SagaStep{ok("a"), ok("b")}}
	if _, err := c.Run(context.Background(), sg, "s1"); err != nil {
		t.Fatal(err)
	}
	// 步骤增删/重排 → 指纹不一致 → 拒绝。
	changed := &Saga{Name: "order", Steps: []SagaStep{ok("a"), ok("b"), ok("c")}}
	if _, err := c.Resume(context.Background(), changed, "s1"); err == nil {
		t.Fatal("指纹不一致必须拒绝恢复")
	} else if !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("意外错误: %v", err)
	}
	reordered := &Saga{Name: "order", Steps: []SagaStep{ok("b"), ok("a")}}
	if _, err := c.Resume(context.Background(), reordered, "s1"); err == nil {
		t.Fatal("重排后必须拒绝恢复")
	}
	// 未变更的定义可恢复（幂等 Resume 终态原样返回）。
	st, err := c.Resume(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompleted {
		t.Fatalf("终态 Resume 原样返回: %q", st.Status)
	}
}

// nil Compensation = 幂等步骤：无需补偿，直接标记 compensated。
func TestSagaNilCompensationStep(t *testing.T) {
	sg := &Saga{
		Name:  "order",
		Steps: []SagaStep{ok("a"), {Name: "idempotent", Action: func(ctx context.Context, ac ActionCtx) error { return nil }}, fail("b")},
	}
	st, err := NewSagaCoordinator(NewInMemorySagaStore()).Run(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompensated {
		t.Fatalf("status = %q", st.Status)
	}
	if st.StepStatuses[1] != StepCompensated {
		t.Fatalf("幂等步骤应标记 compensated: %v", st.StepStatuses)
	}
}

func TestSagaRunRejectsExisting(t *testing.T) {
	store := NewInMemorySagaStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{Name: "order", Steps: []SagaStep{ok("a")}}
	if _, err := c.Run(context.Background(), sg, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Run(context.Background(), sg, "s1"); err == nil {
		t.Fatal("状态已存在必须报错")
	}
}

func TestSagaResumeMissing(t *testing.T) {
	c := NewSagaCoordinator(NewInMemorySagaStore())
	if _, err := c.Resume(context.Background(), &Saga{Name: "x"}, "ghost"); err == nil {
		t.Fatal("不存在的 saga 必须报错")
	}
}

// ctx 取消：以 WithoutCancel 保存状态后返回 ctx.Err()。
func TestSagaCtxCancelSavesWithUncanceledContext(t *testing.T) {
	store := newSpyStore()
	c := NewSagaCoordinator(store)
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{
			ok("a"),
			{
				Name: "hang",
				Action: func(ctx context.Context, ac ActionCtx) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	st, err := c.Run(ctx, sg, "s1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if st != nil {
		t.Fatalf("取消路径返回 nil 状态, got %+v", st)
	}
	// 状态已保存且保存时的 ctx 未被取消。
	saved, _ := store.Load(context.Background(), "s1")
	if saved == nil || saved.Status != StatusRunning {
		t.Fatalf("取消时状态未保存: %+v", saved)
	}
	store.mu.Lock()
	lastCancelled := len(store.cancelled) > 0 && store.cancelled[len(store.cancelled)-1]
	store.mu.Unlock()
	if lastCancelled {
		t.Fatal("取消路径保存必须使用 context.WithoutCancel")
	}
	// Resume 继续：hang 步骤重放（可重入契约），随后完成。
	fixed := &Saga{
		Name: "order",
		Steps: []SagaStep{
			ok("a"),
			{Name: "hang", Action: func(ctx context.Context, ac ActionCtx) error { return nil }},
		},
	}
	// 指纹一致（名称序列未变）。
	final, err := NewSagaCoordinator(store).Resume(context.Background(), fixed, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != StatusCompleted || final.StepStatuses[0] != StepCompleted {
		t.Fatalf("final = %+v", final)
	}
}

// Resume 跳过已 compensated 的步骤：补偿中途崩溃后续跑。
func TestSagaResumeSkipsCompensated(t *testing.T) {
	store := NewInMemorySagaStore()
	// 补偿逆序：先 c、再 b、最后 a。在 b 的补偿处取消 → c 已补偿、
	// b 未完成、a 未开始；Resume 后 b 重入、c 跳过、a 补偿。
	compC := 0
	bHanged := false
	ctx, cancel := context.WithCancel(context.Background())
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{
			{
				Name:         "a",
				Action:       func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error { return nil },
			},
			{
				Name:   "b",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					if !bHanged {
						// 首次补偿 b 时取消 → 模拟补偿中途崩溃；
						// Resume 重放时正常完成（可重入契约）。
						bHanged = true
						cancel()
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				},
			},
			{
				Name:   "c",
				Action: func(ctx context.Context, ac ActionCtx) error { return nil },
				Compensation: func(ctx context.Context, ac ActionCtx) error {
					compC++
					return nil
				},
			},
			fail("d"),
		},
	}
	c := NewSagaCoordinator(store)
	if _, err := c.Run(ctx, sg, "s1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	cancel()

	saved, _ := store.Load(context.Background(), "s1")
	if saved.Status != StatusCompensating {
		t.Fatalf("saved status = %q, want compensating", saved.Status)
	}
	if saved.StepStatuses[2] != StepCompensated {
		t.Fatalf("步骤 c 应已补偿: %v", saved.StepStatuses)
	}

	// Resume：b 的补偿重入，c 不重复补偿。
	final, err := NewSagaCoordinator(store).Resume(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != StatusCompensated {
		t.Fatalf("final = %q", final.Status)
	}
	if compC != 1 {
		t.Fatalf("已 compensated 的步骤不得重复补偿: compC = %d", compC)
	}
}

// Resume 重放 StepRunning 的 Action（可重入契约）。
func TestSagaResumeReplaysRunningStep(t *testing.T) {
	store := NewInMemorySagaStore()
	runs := 0
	ctx, cancel := context.WithCancel(context.Background())
	sg := &Saga{
		Name: "order",
		Steps: []SagaStep{{
			Name: "a",
			Action: func(ctx context.Context, ac ActionCtx) error {
				runs++
				if runs == 1 {
					cancel()
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
		}},
	}
	if _, err := NewSagaCoordinator(store).Run(ctx, sg, "s1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	cancel()
	final, err := NewSagaCoordinator(store).Resume(context.Background(), sg, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != StatusCompleted || runs != 2 {
		t.Fatalf("final = %+v, runs = %d", final, runs)
	}
}

// 内存存储深拷贝隔离。
func TestInMemorySagaStoreIsolation(t *testing.T) {
	store := NewInMemorySagaStore()
	st := &SagaState{
		SagaID:       "s1",
		Status:       StatusRunning,
		StepStatuses: []StepStatus{StepRunning},
		ActionCtx:    ActionCtx{"nested": map[string]any{"k": []any{1, 2}}},
	}
	if err := store.Save(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.Load(context.Background(), "s1")
	loaded.Status = StatusFailed
	loaded.StepStatuses[0] = StepFailed
	loaded.ActionCtx["nested"].(map[string]any)["k"].([]any)[0] = 99
	again, _ := store.Load(context.Background(), "s1")
	if again.Status != StatusRunning || again.StepStatuses[0] != StepRunning {
		t.Fatal("状态经指针外泄")
	}
	if again.ActionCtx["nested"].(map[string]any)["k"].([]any)[0] != 1 {
		t.Fatal("ActionCtx 容器值经引用外泄")
	}
	// Delete 幂等。
	if err := store.Delete(context.Background(), "s1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(context.Background(), "s1"); err != nil {
		t.Fatal(err)
	}
	if loaded, _ := store.Load(context.Background(), "s1"); loaded != nil {
		t.Fatal("删除后应不存在")
	}
}
