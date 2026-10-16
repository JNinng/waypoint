package outbox

import (
	"context"
	"fmt"
	"time"
)

const (
	defaultMaxRetries   = 3
	defaultPollInterval = 5 * time.Second
)

// Dispatcher 拉取 pending/processing 消息并分发到注册的 Handler。
//
// 并发边界：单分发器前提——同一 Store 同时至多一个 Dispatcher；
// DispatchPending 同步调用（在调用方 goroutine 内）。
type Dispatcher struct {
	store        Store
	registry     *HandlerRegistry
	maxRetries   int
	pollInterval time.Duration
	logger       Logger
}

// DispatcherOption 配置 Dispatcher。
type DispatcherOption func(*Dispatcher)

// WithMaxRetries 设置默认重试上限（消息级 MaxRetries 优先；解析下限 1）。
func WithMaxRetries(n int) DispatcherOption {
	return func(d *Dispatcher) { d.maxRetries = n }
}

// WithPollInterval 设置 Start 轮询间隔（默认 5s）。
func WithPollInterval(iv time.Duration) DispatcherOption {
	return func(d *Dispatcher) { d.pollInterval = iv }
}

// WithLogger 注入日志（默认静默）。
func WithLogger(l Logger) DispatcherOption {
	return func(d *Dispatcher) { d.logger = l }
}

// NewDispatcher 创建分发器。
func NewDispatcher(store Store, registry *HandlerRegistry, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		store:        store,
		registry:     registry,
		maxRetries:   defaultMaxRetries,
		pollInterval: defaultPollInterval,
		logger:       nopLogger{},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// DispatchPending 批量分发一次，返回本次成功投递（MarkProcessed）的消息数。
//
// 每条消息：查 Handler（无注册 → MarkFailed）→ 重试上限判定 →
// UpdateStatus(processing) → 调用 Handler(ctx, msg) → 成功 MarkProcessed；
// 失败 retries+1，达上限 MarkFailed 并记录后跳过继续（不阻断同批其余
// 消息），未达上限回退 pending 待下次。
//
// ctx 取消：停止分发并返回 (n, ctx.Err())；处理中崩溃的消息停留在
// processing，下次拉取重取。
func (d *Dispatcher) DispatchPending(ctx context.Context) (int, error) {
	msgs, err := d.store.FetchPending(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox: fetch pending: %w", err)
	}
	delivered := 0
	for _, msg := range msgs {
		if cerr := ctx.Err(); cerr != nil {
			return delivered, cerr
		}
		if done := d.dispatchOne(ctx, msg); done {
			delivered++
		}
	}
	return delivered, nil
}

// dispatchOne 处理单条消息，返回是否成功投递。Store 操作错误记录日志后
// 跳过该消息（不阻断同批其余消息；IO 故障在下一轮重试覆盖）。
func (d *Dispatcher) dispatchOne(ctx context.Context, msg Message) bool {
	handler, ok := d.registry.Get(msg.EventType)
	if !ok {
		if err := d.store.MarkFailed(ctx, msg.ID, fmt.Sprintf("outbox: no handler registered for event type %q", msg.EventType)); err != nil {
			d.logger.Error("outbox: mark failed (no handler)", "id", msg.ID, "error", err)
		}
		return false
	}
	maxRetries := max(1, d.resolveMaxRetries(msg))
	if msg.Retries >= maxRetries {
		// 上限已到（残留 pending/processing）：终态失败出队。
		lastErr := msg.LastError
		if lastErr == "" {
			lastErr = fmt.Sprintf("outbox: retries exhausted (%d)", msg.Retries)
		}
		if err := d.store.MarkFailed(ctx, msg.ID, lastErr); err != nil {
			d.logger.Error("outbox: mark failed (retries exhausted)", "id", msg.ID, "error", err)
		}
		return false
	}
	if err := d.store.UpdateStatus(ctx, msg.ID, StatusProcessing, msg.Retries, msg.LastError); err != nil {
		d.logger.Error("outbox: update status processing", "id", msg.ID, "error", err)
		return false
	}
	if err := handler(ctx, msg); err != nil {
		retries := msg.Retries + 1
		lastErr := err.Error()
		if retries >= maxRetries {
			if merr := d.store.MarkFailed(ctx, msg.ID, lastErr); merr != nil {
				d.logger.Error("outbox: mark failed", "id", msg.ID, "error", merr)
			}
			d.logger.Warn("outbox: message failed permanently", "id", msg.ID, "event_type", msg.EventType, "retries", retries, "error", err)
			return false
		}
		if uerr := d.store.UpdateStatus(ctx, msg.ID, StatusPending, retries, lastErr); uerr != nil {
			d.logger.Error("outbox: update status pending", "id", msg.ID, "error", uerr)
		}
		d.logger.Warn("outbox: message failed, will retry", "id", msg.ID, "event_type", msg.EventType, "retries", retries, "error", err)
		return false
	}
	if err := d.store.MarkProcessed(ctx, msg.ID); err != nil {
		d.logger.Error("outbox: mark processed", "id", msg.ID, "error", err)
		return false
	}
	return true
}

// resolveMaxRetries 解析重试上限：消息级优先，0 = 使用默认值。
func (d *Dispatcher) resolveMaxRetries(msg Message) int {
	if msg.MaxRetries != 0 {
		return msg.MaxRetries
	}
	return d.maxRetries
}

// DispatchUntilIdle 循环 DispatchPending 直到「一轮零投递」或出错，
// 返回累计成功投递条数。
//
// 不保证排空：尚有重试余量的失败消息会留在 pending（本轮无进展即停，
// 紧循环烧重试次数没有意义）。混合负载下的次级效应：只要同轮有其他
// 消息投递成功，循环继续，失败消息的重试余量仍会被连续消耗（分发器
// 无退避）——需要重试间隔请用 Start 轮询或调用方自控节奏。
// ctx 取消：返回已累计条数与 ctx.Err()。
func (d *Dispatcher) DispatchUntilIdle(ctx context.Context) (int, error) {
	delivered := 0
	for {
		n, err := d.DispatchPending(ctx)
		delivered += n
		if err != nil {
			return delivered, err
		}
		if n == 0 {
			return delivered, nil
		}
	}
}

// Start 是后台 ticker 轮询的便利模式（非调度框架）。
// 阻塞直至 ctx 取消；轮询中的分发错误经注入 Logger 记录。
func (d *Dispatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := d.DispatchPending(ctx); err != nil && ctx.Err() == nil {
				d.logger.Error("outbox: dispatch pending", "error", err)
			}
		}
	}
}
