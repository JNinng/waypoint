package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func msg(id string, eventType string) Message {
	return Message{ID: id, TaskID: "t1", EventType: eventType, Payload: json.RawMessage(`{"k":1}`)}
}

func TestAccumulator(t *testing.T) {
	a := &Accumulator{}
	// 空 → nil, nil。
	raw, err := a.MarshalPayload()
	if err != nil || raw != nil {
		t.Fatalf("空累加器 Marshal = (%v, %v)", raw, err)
	}
	if err := a.UnmarshalPayload(nil); err != nil {
		t.Fatal(err)
	}
	m1 := msg("m1", "email")
	if err := a.Add(m1); err != nil {
		t.Fatal(err)
	}
	m2 := msg("m2", "sms")
	if err := a.Add(m2); err != nil {
		t.Fatal(err)
	}
	// 重复 ID → ErrDuplicateID，不得静默丢弃。
	if err := a.Add(msg("m1", "email")); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("重复 ID 必须 ErrDuplicateID: %v", err)
	}
	// Messages 防御性拷贝：外部修改不泄漏。
	got := a.Messages()
	got[0].ID = "HACK"
	if a.Messages()[0].ID != "m1" {
		t.Fatal("内部切片外泄")
	}
	// Marshal / Unmarshal 往返。
	raw, err = a.MarshalPayload()
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["outbox"]; !ok {
		t.Fatalf("命名空间形状错误: %s", raw)
	}
	b := &Accumulator{}
	if err := b.UnmarshalPayload(raw); err != nil {
		t.Fatal(err)
	}
	if len(b.Messages()) != 2 {
		t.Fatalf("往返丢失消息: %v", b.Messages())
	}
	// 非法 JSON → error。
	if err := b.UnmarshalPayload(json.RawMessage(`{bad`)); err == nil {
		t.Fatal("解析失败必须报错")
	}
	// CreatedAt 零值补全。
	c := &Accumulator{}
	zero := Message{ID: "x", EventType: "e"}
	_ = c.Add(zero)
	if c.Messages()[0].CreatedAt.IsZero() {
		t.Fatal("CreatedAt 零值应补全")
	}
}

func TestInMemoryStoreBasics(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	if err := s.Append(ctx, msg("m1", "email")); err != nil {
		t.Fatal(err)
	}
	// ID 重复 → ErrDuplicateID。
	if err := s.Append(ctx, msg("m1", "email")); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("重复 Append 必须 ErrDuplicateID: %v", err)
	}
	if err := s.Append(ctx, msg("m2", "sms")); err != nil {
		t.Fatal(err)
	}
	pending, err := s.FetchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d", len(pending))
	}
	// MarkProcessed 幂等。
	if err := s.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatalf("重复 MarkProcessed 必须成功: %v", err)
	}
	if err := s.MarkProcessed(ctx, "ghost"); err != nil {
		t.Fatalf("不存在的消息视为成功: %v", err)
	}
	// UpdateStatus 不存在 → error。
	if err := s.UpdateStatus(ctx, "ghost", StatusProcessing, 0, ""); err == nil {
		t.Fatal("UpdateStatus 不存在必须报错")
	}
	// completed 不再出现在 pending。
	pending, _ = s.FetchPending(ctx)
	if len(pending) != 1 || pending[0].ID != "m2" {
		t.Fatalf("pending = %v", pending)
	}
	// 值隔离：修改返回值不影响存储内部。
	pending[0].ID = "HACK"
	again, _ := s.FetchPending(ctx)
	if again[0].ID != "m2" {
		t.Fatal("存储内部状态经指针外泄")
	}
}

func TestInMemoryStoreFIFO(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	for i, id := range []string{"c", "a", "b"} {
		m := msg(id, "e")
		m.CreatedAt = base.Add(time.Duration(i) * time.Minute)
		if err := s.Append(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	pending, _ := s.FetchPending(ctx)
	if pending[0].ID != "c" || pending[1].ID != "a" || pending[2].ID != "b" {
		t.Fatalf("必须按 CreatedAt 升序: %v", pending)
	}
}

// 约束 §8-23：Append 与 FetchPending 并发。
func TestInMemoryStoreConcurrentAppendAndFetch(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Append(ctx, msg(string(rune('a'+i)), "e"))
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = s.FetchPending(ctx)
		}()
	}
	wg.Wait()
	pending, err := s.FetchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 8 {
		t.Fatalf("并发 Append 丢失消息: %d", len(pending))
	}
}

func newRegistry(handlers map[string]Handler) *HandlerRegistry {
	r := &HandlerRegistry{}
	for et, h := range handlers {
		r.Register(et, h)
	}
	return r
}

func TestDispatcherDelivers(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()
	var mu sync.Mutex
	var delivered []string
	reg := newRegistry(map[string]Handler{
		"email": func(ctx context.Context, m Message) error {
			mu.Lock()
			delivered = append(delivered, m.ID)
			mu.Unlock()
			return nil
		},
	})
	d := NewDispatcher(store, reg)
	for _, id := range []string{"m1", "m2"} {
		if err := store.Append(ctx, msg(id, "email")); err != nil {
			t.Fatal(err)
		}
	}
	n, err := d.DispatchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(delivered) != 2 {
		t.Fatalf("n = %d, delivered = %v", n, delivered)
	}
	// 全部 completed 后无 pending。
	pending, _ := store.FetchPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("pending = %v", pending)
	}
}

// 处理中崩溃 → processing 重取重投（at-least-once）。
func TestDispatcherRefetchesProcessing(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()
	reg := newRegistry(map[string]Handler{
		"email": func(ctx context.Context, m Message) error { return nil },
	})
	d := NewDispatcher(store, reg)
	if err := store.Append(ctx, msg("m1", "email")); err != nil {
		t.Fatal(err)
	}
	// 模拟「UpdateStatus(processing) 后崩溃」。
	if err := store.UpdateStatus(ctx, "m1", StatusProcessing, 0, ""); err != nil {
		t.Fatal(err)
	}
	pending, err := store.FetchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Status != StatusProcessing {
		t.Fatalf("processing 必须无条件重取: %v", pending)
	}
	n, err := d.DispatchPending(ctx)
	if err != nil || n != 1 {
		t.Fatalf("processing 消息重投失败: n=%d err=%v", n, err)
	}
}

// 毒消息：达上限标 failed 出队且不阻断同批其余消息（约束 §8-19）。
func TestDispatcherPoisonMessageSkipped(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()
	calls := 0
	reg := newRegistry(map[string]Handler{
		"poison": func(ctx context.Context, m Message) error {
			calls++
			return errors.New("always fails")
		},
		"good": func(ctx context.Context, m Message) error { return nil },
	})
	// 默认上限 3。
	if err := store.Append(ctx, msg("bad", "poison")); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, msg("good", "good")); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, reg)
	// 第一轮：bad 失败回 pending（retries=1），good 成功。
	n, err := d.DispatchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("good 必须被投递: n=%d", n)
	}
	// 第二、三轮：bad 耗尽上限 → failed。
	if _, err := d.DispatchPending(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.DispatchPending(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("默认上限 3 次尝试, calls = %d", calls)
	}
	pending, _ := store.FetchPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("毒消息必须出队: %v", pending)
	}
	// 直接查状态（经 UpdateStatus 不可达，用内部 m）。
	failed := false
	store.mu.Lock()
	if m := store.m["bad"]; m.Status == StatusFailed && strings.Contains(m.LastError, "always fails") {
		failed = true
	}
	store.mu.Unlock()
	if !failed {
		t.Fatal("毒消息终态必须为 failed 且记录原因")
	}
}

// 无注册 Handler → MarkFailed。
func TestDispatcherNoHandler(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()
	if err := store.Append(ctx, msg("m1", "unknown")); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, newRegistry(nil))
	n, err := d.DispatchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("n = %d", n)
	}
	store.mu.Lock()
	st := store.m["m1"].Status
	store.mu.Unlock()
	if st != StatusFailed {
		t.Fatalf("无 Handler 消息必须 MarkFailed, status = %q", st)
	}
}

// 消息级 MaxRetries 优先；0 = 默认值；下限 1。
func TestDispatcherMaxRetriesResolution(t *testing.T) {
	cases := []struct {
		name      string
		msgMax    int
		dispMax   int
		wantCalls int
	}{
		{"msg-level 1", 1, 5, 1},
		{"zero-uses-default", 0, 2, 2},
		{"floor-one", 0, 0, 1},
		{"msg-negative-floored", -3, 5, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := NewInMemoryStore()
			m := msg("m1", "e")
			m.MaxRetries = tc.msgMax
			if err := store.Append(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			calls := 0
			reg := newRegistry(map[string]Handler{
				"e": func(ctx context.Context, m Message) error {
					calls++
					return errors.New("boom")
				},
			})
			d := NewDispatcher(store, reg, WithMaxRetries(tc.dispMax))
			for i := 0; i < 10; i++ {
				if _, err := d.DispatchPending(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if calls != tc.wantCalls {
				t.Fatalf("attempts = %d, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestDispatcherCtxCancelStops(t *testing.T) {
	store := NewInMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	reg := newRegistry(map[string]Handler{
		"e": func(ctx context.Context, m Message) error {
			cancel()
			return ctx.Err()
		},
	})
	if err := store.Append(ctx, msg("m1", "e")); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, msg("m2", "e")); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, reg)
	n, err := d.DispatchPending(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n != 0 {
		t.Fatalf("取消后不得计入投递: %d", n)
	}
}

// checklists #4：Start 的取消路径——取消后及时返回、无泄漏。
func TestDispatcherStartCancelPath(t *testing.T) {
	store := NewInMemoryStore()
	reg := newRegistry(map[string]Handler{
		"e": func(ctx context.Context, m Message) error { return nil },
	})
	d := NewDispatcher(store, reg, WithPollInterval(5*time.Millisecond))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Start(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start 在 ctx 取消后未返回（泄漏）")
	}
}

func TestHandlerRegistry(t *testing.T) {
	r := &HandlerRegistry{}
	h := func(ctx context.Context, m Message) error { return nil }
	r.Register("a", h)
	if got, ok := r.Get("a"); !ok || got == nil {
		t.Fatal("注册后必须可查")
	}
	// 覆盖式。
	r.Register("a", func(ctx context.Context, m Message) error { return errors.New("x") })
	if got, _ := r.Get("a"); got == nil {
		t.Fatal("覆盖式注册失败")
	}
	r.Unregister("a")
	if _, ok := r.Get("a"); ok {
		t.Fatal("注销后不得可查")
	}
}
