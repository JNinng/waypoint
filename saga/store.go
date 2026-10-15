package saga

import (
	"context"
	"sync"
)

// InMemoryStore 是内存实现的 SagaStore，用于测试与进程内场景。
// 保存与读取双侧对 map/slice/引用字段深拷贝，内部指针不得外泄；
// 内部加锁，可并发访问（同一 sagaID 的并发推进仍属调用方违约）。
type InMemorySagaStore struct {
	mu sync.Mutex
	m  map[string]*SagaState
}

// NewInMemorySagaStore 创建内存 Store。零值同样可用。
func NewInMemorySagaStore() *InMemorySagaStore {
	return &InMemorySagaStore{m: make(map[string]*SagaState)}
}

// Load 实现 SagaStore：不存在返回 (nil, nil)。
func (s *InMemorySagaStore) Load(ctx context.Context, sagaID string) (*SagaState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.m[sagaID]
	if !ok {
		return nil, nil
	}
	return cloneSagaState(st), nil
}

// Save 实现 SagaStore：完整替换（深拷贝后持有）。
func (s *InMemorySagaStore) Save(ctx context.Context, st *SagaState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]*SagaState)
	}
	s.m[st.SagaID] = cloneSagaState(st)
	return nil
}

// Delete 实现 SagaStore：幂等。
func (s *InMemorySagaStore) Delete(ctx context.Context, sagaID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, sagaID)
	return nil
}

// cloneSagaState 深拷贝执行状态（含 ActionCtx 递归拷贝）。
func cloneSagaState(st *SagaState) *SagaState {
	c := *st
	c.StepStatuses = append([]StepStatus(nil), st.StepStatuses...)
	c.StepErrors = append([]string(nil), st.StepErrors...)
	c.ActionCtx = cloneActionCtx(st.ActionCtx)
	return &c
}

func cloneActionCtx(m ActionCtx) ActionCtx {
	if m == nil {
		return nil
	}
	out := make(ActionCtx, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}
	return out
}

// cloneValue 深拷贝 JSON round-trip 可能出现的容器值。
func cloneValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		return cloneActionCtx(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneValue(e)
		}
		return out
	default:
		return v
	}
}
