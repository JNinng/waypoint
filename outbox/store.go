package outbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Store 是 outbox 消息的持久化抽象。
//
// 契约（ADR-6）：
//   - Append 原子追加；入参为值，实现方拷贝后持有；ID 重复返回
//     ErrDuplicateID。ID 必须是文件系统安全字符串（不含路径分隔符等），
//     文件后端校验。
//   - FetchPending 返回 pending 与 processing 消息（CreatedAt 升序 FIFO），
//     值为拷贝。processing 消息无条件重取——处理中崩溃后下次拉取重投
//     （at-least-once）。前提：单分发器；并发运行多个 Dispatcher 会同时
//     取走同一消息。
//   - MarkProcessed / MarkFailed 幂等：仅「消息不存在」视为成功，
//     IO 故障返回错误。
//   - UpdateStatus 原子更新 status+retries+lastError（重试计数由
//     Dispatcher 维护自增）；消息不存在返回错误。
//   - 实现须支持 Append 与 FetchPending/状态更新并发调用。
type Store interface {
	Append(ctx context.Context, msg Message) error
	FetchPending(ctx context.Context) ([]Message, error)
	MarkProcessed(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, lastError string) error
	UpdateStatus(ctx context.Context, id string, status MessageStatus, retries int, lastErr string) error
}

// InMemoryStore 是内存实现的 Store，用于测试与进程内场景。
// 保存与读取双侧对 map/引用字段深拷贝，内部状态不经指针外泄；
// 内部加锁，支持并发 Append 与 FetchPending。
type InMemoryStore struct {
	mu sync.Mutex
	m  map[string]Message
}

// NewInMemoryStore 创建内存 Store。零值同样可用。
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{m: make(map[string]Message)}
}

// Append 实现 Store。
func (s *InMemoryStore) Append(ctx context.Context, msg Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]Message)
	}
	if _, exists := s.m[msg.ID]; exists {
		return fmt.Errorf("outbox: append %s: %w", msg.ID, ErrDuplicateID)
	}
	m := cloneMessage(msg)
	m.normalize()
	s.m[msg.ID] = m
	return nil
}

// FetchPending 实现 Store：pending 与 processing，CreatedAt 升序 FIFO。
func (s *InMemoryStore) FetchPending(ctx context.Context) ([]Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Message
	for _, m := range s.m {
		if m.Status == StatusPending || m.Status == StatusProcessing {
			out = append(out, cloneMessage(m))
		}
	}
	sortMessages(out)
	return out, nil
}

// MarkProcessed 实现 Store（幂等：消息不存在视为成功）。
func (s *InMemoryStore) MarkProcessed(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.m[id]
	if !ok {
		return nil
	}
	m.Status = StatusCompleted
	now := time.Now().UTC()
	m.DeliveredAt = &now
	s.m[id] = m
	return nil
}

// MarkFailed 实现 Store（幂等：消息不存在视为成功）。
func (s *InMemoryStore) MarkFailed(ctx context.Context, id string, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.m[id]
	if !ok {
		return nil
	}
	m.Status = StatusFailed
	m.LastError = lastError
	s.m[id] = m
	return nil
}

// UpdateStatus 实现 Store（消息不存在返回错误）。
func (s *InMemoryStore) UpdateStatus(ctx context.Context, id string, status MessageStatus, retries int, lastErr string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.m[id]
	if !ok {
		return fmt.Errorf("outbox: update status: message %s not found", id)
	}
	m.Status = status
	m.Retries = retries
	m.LastError = lastErr
	s.m[id] = m
	return nil
}

// sortMessages 按 CreatedAt 升序（相同时间按 ID）排序，保证 FIFO 确定性。
func sortMessages(msgs []Message) {
	sort.SliceStable(msgs, func(i, j int) bool {
		if !msgs[i].CreatedAt.Equal(msgs[j].CreatedAt) {
			return msgs[i].CreatedAt.Before(msgs[j].CreatedAt)
		}
		return msgs[i].ID < msgs[j].ID
	})
}
