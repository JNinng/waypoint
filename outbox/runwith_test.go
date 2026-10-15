package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
)

type memRepo struct {
	mu    sync.Mutex
	m     map[string][]byte
	saves int
}

func (r *memRepo) Load(ctx context.Context, taskID string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.m[taskID]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), data...), nil
}

func (r *memRepo) Save(ctx context.Context, taskID string, s []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.m[taskID] = append([]byte(nil), s...)
	return nil
}

func (r *memRepo) Delete(ctx context.Context, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, taskID)
	return nil
}

// outboxEngine：一步完成任务，并把累加的消息写入 Payload 的 outbox 命名空间。
type outboxEngine struct {
	acc  *Accumulator
	msgs []Message
}

func (e *outboxEngine) TaskType() string        { return "notify" }
func (e *outboxEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (e *outboxEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	switch s.Phase {
	case state.PhasePending:
		s.Phase = state.PhaseRunning
		return 0, nil
	default:
		for _, m := range e.msgs {
			if err := e.acc.Add(m); err != nil {
				return 0, err
			}
		}
		raw, err := e.acc.MarshalPayload()
		if err != nil {
			return 0, err
		}
		if raw != nil {
			// MarshalPayload 返回 {"outbox":[...]} 命名空间块；
			// 合并进 Payload 顶层时取内层数组（框架不提供合并助手）。
			var ns map[string]json.RawMessage
			if err := json.Unmarshal(raw, &ns); err != nil {
				return 0, err
			}
			var top map[string]json.RawMessage
			if len(s.Payload) > 0 {
				if err := json.Unmarshal(s.Payload, &top); err != nil {
					return 0, err
				}
			}
			if top == nil {
				top = map[string]json.RawMessage{}
			}
			top["outbox"] = ns["outbox"]
			out, err := json.Marshal(top)
			if err != nil {
				return 0, err
			}
			s.Payload = out
		}
		s.Phase = state.PhaseCompleted
		return 1, nil
	}
}

// 设计 §6.3 / §5.6：Run 成功后意图对齐到 Store。
func TestRunWithOutboxAlignsMessages(t *testing.T) {
	repo := &memRepo{m: map[string][]byte{}}
	store := NewInMemoryStore()
	eng := &outboxEngine{acc: &Accumulator{}, msgs: []Message{
		msg("m1", "email"),
		msg("m2", "sms"),
	}}
	final, err := RunWithOutbox(context.Background(), repo, eng, "t1", store)
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
	if len(pending) != 2 {
		t.Fatalf("意图未对齐到 Store: %v", pending)
	}
}

// 恢复窗口：「Run 成功但 Append 前崩溃」——重调 RunWithOutbox 幂等恢复
// （ErrDuplicateID 即跳过；已完成任务不重复执行副作用）。
func TestRunWithOutboxRecoveryWindowIdempotent(t *testing.T) {
	repo := &memRepo{m: map[string][]byte{}}
	store := NewInMemoryStore()
	eng := &outboxEngine{acc: &Accumulator{}, msgs: []Message{msg("m1", "email")}}

	// 第一次：Run 成功 + Append 成功。
	if _, err := RunWithOutbox(context.Background(), repo, eng, "t1", store); err != nil {
		t.Fatal(err)
	}
	// 第二次：模拟恢复重试（Run 已 completed → 直接返回最终状态，
	// Append 撞 ErrDuplicateID → 幂等跳过）。
	final, err := RunWithOutbox(context.Background(), repo, eng, "t1", store)
	if err != nil {
		t.Fatalf("恢复窗口必须幂等: %v", err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	pending, _ := store.FetchPending(context.Background())
	if len(pending) != 1 {
		t.Fatalf("重复 Append 不得产生重复消息: %v", pending)
	}
}

// Run 失败 → 不对齐、返回错误。
func TestRunWithOutboxRunError(t *testing.T) {
	repo := &memRepo{m: map[string][]byte{}}
	store := NewInMemoryStore()
	eng := &failingEngine{}
	_, err := RunWithOutbox(context.Background(), repo, eng, "t1", store)
	if err == nil {
		t.Fatal("Run 失败必须返回错误")
	}
	pending, _ := store.FetchPending(context.Background())
	if len(pending) != 0 {
		t.Fatalf("Run 失败不得对齐: %v", pending)
	}
}

type failingEngine struct{}

func (e *failingEngine) TaskType() string        { return "notify" }
func (e *failingEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (e *failingEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	return 0, errors.New("transient")
}

// Payload 的 outbox 命名空间非法 → 显式报错。
func TestRunWithOutboxInvalidNamespace(t *testing.T) {
	repo := &memRepo{m: map[string][]byte{}}
	// 预置 completed 状态且 outbox 命名空间为非数组。
	payload, _ := json.Marshal(map[string]json.RawMessage{"outbox": json.RawMessage(`42`)})
	seed := state.BaseTaskState{TaskID: "t1", TaskType: "notify", Phase: state.PhaseCompleted, Payload: payload}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data

	eng := &outboxEngine{acc: &Accumulator{}}
	store := NewInMemoryStore()
	_, err := RunWithOutbox(context.Background(), repo, eng, "t1", store)
	if err == nil || !strings.Contains(err.Error(), "parse outbox namespace") {
		t.Fatalf("非法命名空间必须报错: %v", err)
	}
}
