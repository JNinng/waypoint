package outbox

import (
	"context"
	"errors"
	"testing"
)

// 字面量构造与 Register 循环等价。
func TestNewHandlerRegistryEquivalence(t *testing.T) {
	h := func(ctx context.Context, m Message) error { return nil }
	literal := NewHandlerRegistry(map[string]Handler{"a": h, "b": h})

	var manual *HandlerRegistry
	manual = &HandlerRegistry{}
	manual.Register("a", h)
	manual.Register("b", h)

	for _, et := range []string{"a", "b"} {
		got, ok1 := literal.Get(et)
		want, ok2 := manual.Get(et)
		if ok1 != ok2 || got == nil || want == nil {
			t.Fatalf("路由表 %q 不一致", et)
		}
	}
	if _, ok := literal.Get("c"); ok {
		t.Fatal("不应有额外路由")
	}
	// nil map = 空表。
	if r := NewHandlerRegistry(nil); r == nil {
		t.Fatal("nil map 应构造空表")
	}
}

// 停止语义：无进展即停——失败消息带重试余量留在 pending，不排空。
func TestDispatchUntilIdleStopsWithoutProgress(t *testing.T) {
	store := NewInMemoryStore()
	calls := 0
	dispatcher := NewDispatcher(store, NewHandlerRegistry(map[string]Handler{
		"ok": func(ctx context.Context, m Message) error { return nil },
		"flaky": func(ctx context.Context, m Message) error {
			calls++
			return errors.New("line busy")
		},
	}), WithMaxRetries(3))
	ctx := context.Background()
	for _, id := range []string{"m1", "m2"} {
		if err := store.Append(ctx, Message{ID: id, EventType: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Append(ctx, Message{ID: "bad", EventType: "flaky"}); err != nil {
		t.Fatal(err)
	}

	n, err := dispatcher.DispatchUntilIdle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 轮 1：m1、m2 投递成功，bad 尝试 1 次失败（有进展，继续）；
	// 轮 2：bad 尝试 2 次失败（零投递，停止）。
	if n != 2 {
		t.Fatalf("delivered = %d, want 2", n)
	}
	if calls != 2 {
		t.Fatalf("失败消息被烧了 %d 次尝试, want 2", calls)
	}
	pending, _ := store.FetchPending(ctx)
	if len(pending) != 1 || pending[0].ID != "bad" {
		t.Fatalf("失败消息应带余量留在 pending: %v", pending)
	}
	if pending[0].Retries != 2 {
		t.Fatalf("retries = %d, want 2", pending[0].Retries)
	}
}

// 全健康负载：一轮投完，下一轮零投递自然停止。
func TestDispatchUntilIdleDrainsHealthy(t *testing.T) {
	store := NewInMemoryStore()
	dispatcher := NewDispatcher(store, NewHandlerRegistry(map[string]Handler{
		"e": func(ctx context.Context, m Message) error { return nil },
	}))
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := store.Append(ctx, Message{ID: id, EventType: "e"}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := dispatcher.DispatchUntilIdle(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("delivered = %d", n)
	}
	pending, _ := store.FetchPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("pending = %v", pending)
	}
}

// ctx 取消：轮间的 ctx 检查捕获取消，返回已累计条数与 ctx.Err()。
func TestDispatchUntilIdleCtxCancel(t *testing.T) {
	store := NewInMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher := NewDispatcher(store, NewHandlerRegistry(map[string]Handler{
		"e": func(ctx context.Context, m Message) error {
			cancel()
			return ctx.Err()
		},
	}))
	for _, id := range []string{"m1", "m2"} {
		if err := store.Append(ctx, Message{ID: id, EventType: "e"}); err != nil {
			t.Fatal(err)
		}
	}
	n, err := dispatcher.DispatchUntilIdle(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if n != 0 {
		t.Fatalf("取消后不得计入投递: %d", n)
	}
}
