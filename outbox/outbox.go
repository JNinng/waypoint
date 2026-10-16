// Package outbox 实现调度层 outbox 模式：不可逆操作（发邮件、扣款、
// 消息队列发送）不得在引擎 Execute 内直接执行——意图先随 checkpoint 原子
// 持久化，再由调度层以 at-least-once 分发。
//
// 角色划分：
//   - Accumulator：引擎内增量收集消息，序列化进 Payload 的 outbox 命名空间
//     （随 checkpoint 原子落盘）。
//   - Store：消息持久化抽象；FetchPending 返回 pending 与 processing 消息
//     （processing 无条件重取——处理中崩溃后下次拉取重投，at-least-once）。
//   - Dispatcher：拉取并分发到幂等 Handler；重试上限解析带下限 1。
//   - RunWithOutbox：桥接入口——运行引擎并把最终状态的 outbox 命名空间
//     对齐到 Store（恢复契约的唯一实现）。
//
// 并发边界（ADR-6）：单分发器前提——同一 Store 同时至多一个 Dispatcher；
// Store 实现须支持 Append 与 FetchPending/状态更新并发调用（典型用法：
// 后台 Dispatcher 轮询期间，前台任务完成并 Append 新消息）。
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// MessageStatus 是消息生命周期状态。
type MessageStatus string

const (
	StatusPending    MessageStatus = "pending"
	StatusProcessing MessageStatus = "processing"
	StatusCompleted  MessageStatus = "completed"
	StatusFailed     MessageStatus = "failed"
)

// ErrDuplicateID 表示消息 ID 已存在。重复 Append（恢复重试）以本哨兵
// 幂等跳过。
var ErrDuplicateID = errors.New("waypoint/outbox: duplicate message id")

// Message 是一条待分发的意图记录。
//
// 值语义：Message 在 Append 入参、FetchPending 返回、Handler 入参、
// Accumulator.Messages 返回处全链路按值传递/拷贝——存储内部状态不经
// 指针外泄。
type Message struct {
	ID          string          `json:"id"`
	TaskID      string          `json:"task_id"`
	EventType   string          `json:"event_type"` // 路由 key，纯业务字符串
	Payload     json.RawMessage `json:"payload,omitempty"`
	Status      MessageStatus   `json:"status"`
	Retries     int             `json:"retries"`
	MaxRetries  int             `json:"max_retries,omitempty"` // 重试上限按「最大尝试次数（含首次）」解析；0 = 使用 Dispatcher 默认值
	CreatedAt   time.Time       `json:"created_at"`
	LastError   string          `json:"last_error,omitempty"`
	DeliveredAt *time.Time      `json:"delivered_at,omitempty"`
}

// cloneMessage 深拷贝消息（含 Payload 字节）。
func cloneMessage(m Message) Message {
	c := m
	if m.Payload != nil {
		c.Payload = append(json.RawMessage(nil), m.Payload...)
	}
	if m.DeliveredAt != nil {
		t := *m.DeliveredAt
		c.DeliveredAt = &t
	}
	return c
}

// Normalize 补全默认字段：Status 零值置 pending，CreatedAt 零值置当前时间。
// 后端在 Append 时调用；Accumulator.Add 与内存后端同样经此补全。
func (m *Message) Normalize() {
	if m.Status == "" {
		m.Status = StatusPending
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
}

// Handler 处理一条消息。必须幂等：同一条消息可能被投递多次。
// 入参为值——Handler 只读，状态迁移经 Store 进行。
type Handler func(ctx context.Context, msg Message) error

// HandlerRegistry 是 EventType → Handler 的路由表。
// 内部加锁，Register/Unregister/Get 可并发调用；但分发开始后才动态变更
// Handler 语义（同 ID 换实现）由调用方自行串行化。
// Register 覆盖式注册；零值可用。
type HandlerRegistry struct {
	mu sync.RWMutex
	m  map[string]Handler
}

// NewHandlerRegistry 以字面量构造路由表；增量注册仍用 Register。
func NewHandlerRegistry(handlers map[string]Handler) *HandlerRegistry {
	r := &HandlerRegistry{}
	for eventType, h := range handlers {
		r.Register(eventType, h)
	}
	return r
}

// Register 覆盖式注册 eventType 的 Handler。
func (r *HandlerRegistry) Register(eventType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = make(map[string]Handler)
	}
	r.m[eventType] = h
}

// Unregister 注销 eventType 的 Handler。
func (r *HandlerRegistry) Unregister(eventType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, eventType)
}

// Get 返回 eventType 的 Handler；未注册时 ok = false。
func (r *HandlerRegistry) Get(eventType string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.m[eventType]
	return h, ok
}

// Logger 是注入式日志接口（*log/slog.Logger 满足之）。默认静默。
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
