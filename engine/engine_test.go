package engine

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jninng/waypoint/state"
)

// memRepo 是测试用内存 StateRepository（按 taskID 隔离）。
type memRepo struct {
	mu    sync.Mutex
	m     map[string][]byte
	saves int
	// failSaveOnNth 非 0 时，第 N 次 Save 返回错误（模拟「Execute 成功但
	// checkpoint 未保存」的崩溃窗口）。
	failSaveOnNth int
}

func newMemRepo() *memRepo { return &memRepo{m: map[string][]byte{}} }

func (r *memRepo) Load(ctx context.Context, taskID string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.m[taskID]
	if !ok {
		return nil, nil
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (r *memRepo) Save(ctx context.Context, taskID string, s []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saves++
	if r.failSaveOnNth != 0 && r.saves == r.failSaveOnNth {
		return errors.New("boom: simulated save failure")
	}
	out := make([]byte, len(s))
	copy(out, s)
	r.m[taskID] = out
	return nil
}

func (r *memRepo) Delete(ctx context.Context, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, taskID)
	return nil
}

func (r *memRepo) load(t *testing.T, taskID string) *state.BaseTaskState {
	t.Helper()
	data, err := r.Load(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		return nil
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return &s
}

// stepEngine 按脚本驱动：每步可推进 Phase、返回 LSN、注入错误或污染状态。
type stepEngine struct {
	taskType string
	steps    []step
	calls    int
}

type step struct {
	phase state.Phase
	lsn   state.LSN
	err   error
	// dirty 在返回 err 前修改状态，用于验证快照回滚。
	dirty func(s *state.BaseTaskState)
}

func (e *stepEngine) TaskType() string { return e.taskType }
func (e *stepEngine) Phases() PhaseSet { return DefaultPhaseSet() }
func (e *stepEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	i := e.calls
	if i >= len(e.steps) {
		return 0, errors.New("engine: no more steps")
	}
	st := e.steps[i]
	e.calls++
	if st.dirty != nil {
		st.dirty(s)
	}
	if st.err != nil {
		return 0, st.err
	}
	if st.phase != "" {
		s.Phase = st.phase
	}
	return st.lsn, nil
}

func TestRunNewTaskLifecycle(t *testing.T) {
	repo := newMemRepo()
	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 10},
			{phase: state.PhaseRunning, lsn: 20},
			{phase: state.PhaseCompleted, lsn: 30},
		},
	}
	final, err := Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("final phase = %q", final.Phase)
	}
	if final.CheckpointLSN != 30 {
		t.Fatalf("final lsn = %d", final.CheckpointLSN)
	}
	if final.TaskType != "etl" || final.TaskID != "t1" {
		t.Fatalf("初始化字段缺失: %+v", final)
	}
	// 返回值与持久化一致。
	persisted := repo.load(t, "t1")
	if persisted.Phase != state.PhaseCompleted || persisted.CheckpointLSN != 30 {
		t.Fatalf("持久化状态 = %+v", persisted)
	}
}

func TestRunResumesFromPersistedState(t *testing.T) {
	repo := newMemRepo()
	seed := state.BaseTaskState{
		TaskID: "t1", TaskType: "etl", Phase: state.PhaseRunning, CheckpointLSN: 10,
	}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data

	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseCompleted, lsn: 20},
		},
	}
	final, err := Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.CheckpointLSN != 20 {
		t.Fatalf("lsn = %d", final.CheckpointLSN)
	}
}

func TestRunRejectsTaskTypeMismatch(t *testing.T) {
	repo := newMemRepo()
	seed := state.BaseTaskState{TaskID: "t1", TaskType: "export", Phase: state.PhaseRunning}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data
	eng := &stepEngine{taskType: "ingest"}
	if _, err := Run(context.Background(), repo, eng, "t1"); err == nil {
		t.Fatal("TaskType 不匹配必须拒绝恢复")
	} else if !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("意外错误: %v", err)
	}
}

func TestRunRejectsUnknownPhase(t *testing.T) {
	repo := newMemRepo()
	seed := state.BaseTaskState{TaskID: "t1", TaskType: "etl", Phase: state.Phase("weird")}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data
	eng := &stepEngine{taskType: "etl"}
	if _, err := Run(context.Background(), repo, eng, "t1"); err == nil {
		t.Fatal("未知 Phase 必须拒绝恢复")
	}
}

func TestPhaseSetValidation(t *testing.T) {
	cases := []struct {
		name string
		ps   PhaseSet
	}{
		{"empty All", PhaseSet{}},
		{"initial not in All", PhaseSet{
			All: []state.Phase{state.PhaseRunning}, Initial: state.PhasePending}},
		{"initial terminal", PhaseSet{
			All:      []state.Phase{state.PhasePending, state.PhaseCompleted},
			Initial:  state.PhasePending,
			Terminal: []state.Phase{state.PhasePending}}},
		{"recoverable not in All", PhaseSet{
			All:         []state.Phase{state.PhasePending},
			Recoverable: []state.Phase{state.PhaseRunning}}},
		{"terminal not in All", PhaseSet{
			All:      []state.Phase{state.PhasePending},
			Terminal: []state.Phase{state.PhaseCompleted}}},
		{"failed not terminal", PhaseSet{
			All:      []state.Phase{state.PhasePending, state.PhaseRunning, state.PhaseFailed},
			Terminal: []state.Phase{state.PhaseRunning}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.ps.validate(); err == nil {
				t.Fatal("期望校验失败")
			}
		})
	}
	if err := DefaultPhaseSet().validate(); err != nil {
		t.Fatalf("DefaultPhaseSet 必须合法: %v", err)
	}
}

// 优雅失败：引擎置 PhaseFailed 返回 nil → 唯一落盘失败路径。
func TestRunGracefulFailure(t *testing.T) {
	repo := newMemRepo()
	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 1},
			{phase: state.PhaseFailed, lsn: 1, dirty: func(s *state.BaseTaskState) {
				s.Message = "数据源不可用"
			}},
		},
	}
	final, err := Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatalf("优雅失败不是 error: %v", err)
	}
	if final.Phase != state.PhaseFailed {
		t.Fatalf("phase = %q", final.Phase)
	}
	persisted := repo.load(t, "t1")
	if persisted == nil || persisted.Phase != state.PhaseFailed || persisted.Message != "数据源不可用" {
		t.Fatalf("failed 必须已落盘: %+v", persisted)
	}
}

// ADR-5：重试以快照为起点，失败调用的部分修改不得泄漏。
func TestRunRetryRollsBackToSnapshot(t *testing.T) {
	repo := newMemRepo()
	var seenPhases []state.Phase
	var seenMsgs []string
	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 1},
			{err: errors.New("transient"), dirty: func(s *state.BaseTaskState) {
				s.Message = "污染"
				s.Phase = state.PhaseFailed
				s.Payload = json.RawMessage(`{"polluted":true}`)
			}},
			{phase: state.PhaseCompleted, lsn: 2},
		},
	}
	// 记录每次 Execute 入口看到的状态。
	wrapped := &observingEngine{inner: eng, observe: func(s *state.BaseTaskState) {
		seenPhases = append(seenPhases, s.Phase)
		seenMsgs = append(seenMsgs, s.Message)
	}}
	final, err := Run(context.Background(), repo, wrapped, "t1", WithRetry(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	if len(seenPhases) != 3 {
		t.Fatalf("Execute 次数 = %d", len(seenPhases))
	}
	if seenPhases[1] != state.PhaseRunning || seenMsgs[1] != "" {
		t.Fatalf("重试必须看到回滚后的状态: phase=%q msg=%q", seenPhases[1], seenMsgs[1])
	}
}

type observingEngine struct {
	inner   Engine
	observe func(s *state.BaseTaskState)
}

func (e *observingEngine) TaskType() string { return e.inner.TaskType() }
func (e *observingEngine) Phases() PhaseSet { return e.inner.Phases() }
func (e *observingEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	cp := cloneState(s)
	e.observe(cp)
	return e.inner.Execute(ctx, s)
}

// 可重试错误不落盘：重试耗尽后任务停留于最后已提交阶段。
func TestRunRetryExhaustedDoesNotSave(t *testing.T) {
	repo := newMemRepo()
	boom := errors.New("transient")
	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 1},
			{err: boom, dirty: func(s *state.BaseTaskState) { s.Phase = state.PhaseFailed }},
			{err: boom},
			{err: boom},
		},
	}
	_, err := Run(context.Background(), repo, eng, "t1", WithRetry(1, time.Millisecond))
	if !errors.Is(err, boom) {
		t.Fatalf("重试耗尽必须返回原始错误: %v", err)
	}
	persisted := repo.load(t, "t1")
	if persisted == nil || persisted.Phase != state.PhaseRunning || persisted.CheckpointLSN != 1 {
		t.Fatalf("失败不落盘，停留最后已提交阶段: %+v", persisted)
	}

	// 下次 Run 从该阶段恢复。
	eng2 := &stepEngine{
		taskType: "etl",
		steps:    []step{{phase: state.PhaseCompleted, lsn: 2}},
	}
	final, err := Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.CheckpointLSN != 2 {
		t.Fatalf("恢复后 lsn = %d", final.CheckpointLSN)
	}
}

func TestRunSaveErrorNotRetried(t *testing.T) {
	repo := newMemRepo()
	repo.failSaveOnNth = 2
	eng := &stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 1},
			{phase: state.PhaseCompleted, lsn: 2},
		},
	}
	_, err := Run(context.Background(), repo, eng, "t1", WithRetry(3, 0))
	if err == nil {
		t.Fatal("Save 错误必须直接返回")
	}
	if eng.calls != 2 {
		t.Fatalf("Save 错误不得触发 Execute 重试, calls = %d", eng.calls)
	}
}

func TestRunCtxCancelReturnsCtxErr(t *testing.T) {
	repo := newMemRepo()
	ctx, cancel := context.WithCancel(context.Background())
	eng := &cancelEngine{taskType: "etl", cancel: cancel}
	_, err := Run(ctx, repo, eng, "t1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if repo.load(t, "t1") != nil {
		t.Fatal("取消路径不得落盘")
	}
}

type cancelEngine struct {
	taskType string
	cancel   context.CancelFunc
	calls    int
}

func (e *cancelEngine) TaskType() string { return e.taskType }
func (e *cancelEngine) Phases() PhaseSet { return DefaultPhaseSet() }
func (e *cancelEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	e.calls++
	e.cancel()
	return 0, ctx.Err()
}

// alignEngine 记录 Align 调用顺序，用于验证恢复编排。
type alignEngine struct {
	stepEngine
	events     []string
	alignTo    state.LSN
	notAligner bool
}

func (e *alignEngine) Align(ctx context.Context, target state.LSN) error {
	e.events = append(e.events, "align")
	e.alignTo = target
	return nil
}

func (e *alignEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	e.events = append(e.events, "execute")
	return e.stepEngine.Execute(ctx, s)
}

func TestRunAlignsBeforeExecuteOnRecovery(t *testing.T) {
	repo := newMemRepo()
	seed := state.BaseTaskState{TaskID: "t1", TaskType: "etl", Phase: state.PhaseRunning, CheckpointLSN: 42}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data

	eng := &alignEngine{stepEngine: stepEngine{
		taskType: "etl",
		steps:    []step{{phase: state.PhaseCompleted, lsn: 50}},
	}}
	if _, err := Run(context.Background(), repo, eng, "t1"); err != nil {
		t.Fatal(err)
	}
	if len(eng.events) != 2 || eng.events[0] != "align" || eng.events[1] != "execute" {
		t.Fatalf("Align 必须先于 Execute: %v", eng.events)
	}
	if eng.alignTo != 42 {
		t.Fatalf("Align target = %d, want 42", eng.alignTo)
	}
}

func TestRunSkipsAlignForNewTaskAndTerminal(t *testing.T) {
	repo := newMemRepo()
	// 新任务（pending ∉ Recoverable）不 Align。
	eng := &alignEngine{stepEngine: stepEngine{
		taskType: "etl",
		steps:    []step{{phase: state.PhaseCompleted, lsn: 1}},
	}}
	if _, err := Run(context.Background(), repo, eng, "t1"); err != nil {
		t.Fatal(err)
	}
	for _, ev := range eng.events {
		if ev == "align" {
			t.Fatal("新任务不得触发 Align")
		}
	}

	// 终态任务（completed 同时在 Recoverable 中——终态优先）不 Align、不 Execute。
	ps := DefaultPhaseSet()
	ps.Recoverable = []state.Phase{state.PhaseRunning, state.PhaseCompleted}
	eng2 := &alignEngineWithSet{alignEngine: alignEngine{stepEngine: stepEngine{
		taskType: "etl",
		steps:    []step{{phase: state.PhaseCompleted, lsn: 1}},
	}}, ps: ps}
	seed := state.BaseTaskState{TaskID: "t2", TaskType: "etl", Phase: state.PhaseCompleted}
	data, _ := json.Marshal(&seed)
	repo.m["t2"] = data
	final, err := Run(context.Background(), repo, eng2, "t2")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted || len(eng2.events) != 0 {
		t.Fatalf("终态任务直接返回: %+v events=%v", final, eng2.events)
	}
}

type alignEngineWithSet struct {
	alignEngine
	ps PhaseSet
}

func (e *alignEngineWithSet) Phases() PhaseSet { return e.ps }

func TestRunExecuteUnknownPhaseRejected(t *testing.T) {
	repo := newMemRepo()
	eng := &stepEngine{
		taskType: "etl",
		steps:    []step{{phase: state.Phase("bogus"), lsn: 1}},
	}
	_, err := Run(context.Background(), repo, eng, "t1")
	if err == nil || !strings.Contains(err.Error(), "not in PhaseSet.All") {
		t.Fatalf("引擎写入未知阶段必须报错: %v", err)
	}
	if repo.load(t, "t1") != nil {
		t.Fatal("未知阶段不得落盘")
	}
}

// progressEngine 断言 Progressor 被消费。
type progressEngine struct {
	stepEngine
	progress int
}

func (e *progressEngine) Progress(s state.BaseTaskState) int { return e.progress }

func TestRunWritesProgress(t *testing.T) {
	repo := newMemRepo()
	eng := &progressEngine{stepEngine: stepEngine{
		taskType: "etl",
		steps: []step{
			{phase: state.PhaseRunning, lsn: 1},
			{phase: state.PhaseCompleted, lsn: 2},
		},
	}, progress: 77}
	final, err := Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Progress != 77 {
		t.Fatalf("progress = %d", final.Progress)
	}
}

func TestWithRetryPanicsOnNegative(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("负值参数必须 panic")
		}
	}()
	WithRetry(-1, 0)
}
