package waypoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/state"
)

// stepEngine 与 engine 包测试同构的脚本引擎。
type stepEngine struct {
	taskType string
	steps    []step
	calls    int
}

type step struct {
	phase state.Phase
	lsn   state.LSN
	err   error
}

func (e *stepEngine) TaskType() string        { return e.taskType }
func (e *stepEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (e *stepEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	i := e.calls
	if i >= len(e.steps) {
		return 0, errors.New("no more steps")
	}
	st := e.steps[i]
	e.calls++
	if st.err != nil {
		return 0, st.err
	}
	if st.phase != "" {
		s.Phase = st.phase
	}
	return st.lsn, nil
}

func completeEngine(taskType string) *stepEngine {
	return &stepEngine{taskType: taskType, steps: []step{
		{phase: state.PhaseRunning, lsn: 1},
		{phase: state.PhaseCompleted, lsn: 2},
	}}
}

// 糖与显式路径等价：NewTask+Run == filestore.New+engine.Run。
func TestTaskRunEquivalence(t *testing.T) {
	base := t.TempDir()

	// 显式路径。
	repo, err := filestore.New(filepath.Join(base, "explicit"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := engine.Run(context.Background(), repo, completeEngine("etl"), "t1")
	if err != nil {
		t.Fatal(err)
	}

	// 糖。
	got, err := NewTask(filepath.Join(base, "sugar"), completeEngine("etl"), "t1").Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != want.Phase || got.CheckpointLSN != want.CheckpointLSN || got.TaskType != want.TaskType {
		t.Fatalf("糖与显式路径结果不一致: %+v vs %+v", got, want)
	}
	// 状态确实落在绑定目录。
	if _, err := os.Stat(filepath.Join(base, "sugar", "t1.state")); err != nil {
		t.Fatalf("状态未写入绑定目录: %v", err)
	}
}

// dir 构造错误延迟到 Run 才暴露（构造不触盘），错误带包前缀。
func TestTaskRunDirErrorSurfacesAtRun(t *testing.T) {
	base := t.TempDir()
	blocker := filepath.Join(base, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	task := NewTask(filepath.Join(blocker, "states"), completeEngine("etl"), "t1") // 构造不报错
	_, err := task.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "waypoint:") {
		t.Fatalf("dir 错误应在 Run 时以 waypoint: 前缀返回: %v", err)
	}
}

// 同一 Task 二次 Run 即恢复同一世系。
func TestTaskRunResumesSameLineage(t *testing.T) {
	dir := t.TempDir()
	task := NewTask(dir, completeEngine("etl"), "t1")
	if _, err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	final, err := task.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted || final.CheckpointLSN != 2 {
		t.Fatalf("final = %+v", final)
	}
}

// RunWithOutbox 对齐意图且重调幂等（恢复窗口）。
func TestTaskRunWithOutbox(t *testing.T) {
	dir := t.TempDir()
	store := outbox.NewInMemoryStore()
	eng := &outboxNotifyEngine{acc: &outbox.Accumulator{}}

	task := NewTask(dir, eng, "notify-1")
	final, err := task.RunWithOutbox(context.Background(), store)
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	pending, err := store.FetchPending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("意图未对齐: %v", pending)
	}
	// 重调：Append 撞重复 ID 幂等跳过。
	if _, err := task.RunWithOutbox(context.Background(), store); err != nil {
		t.Fatal(err)
	}
	pending, _ = store.FetchPending(context.Background())
	if len(pending) != 1 {
		t.Fatalf("重调产生重复消息: %v", pending)
	}
}

// outboxNotifyEngine 是最小通知引擎（与 outbox 包测试同构）。
type outboxNotifyEngine struct{ acc *outbox.Accumulator }

func (e *outboxNotifyEngine) TaskType() string        { return "notify" }
func (e *outboxNotifyEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (e *outboxNotifyEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	if err := e.acc.Add(outbox.Message{ID: "m1", TaskID: s.TaskID, EventType: "email"}); err != nil {
		return 0, err
	}
	raw, err := e.acc.MarshalPayload()
	if err != nil {
		return 0, err
	}
	var ns map[string]json.RawMessage
	if err := json.Unmarshal(raw, &ns); err != nil {
		return 0, err
	}
	top := map[string]json.RawMessage{"outbox": ns["outbox"]}
	out, err := json.Marshal(top)
	if err != nil {
		return 0, err
	}
	s.Payload = out
	s.Phase = state.PhaseCompleted
	return 1, nil
}
