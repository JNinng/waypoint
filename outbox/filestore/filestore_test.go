package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jninng/waypoint/outbox"
)

func timeAdd(minutes int) time.Time {
	return time.Now().UTC().Add(time.Duration(minutes) * time.Minute)
}

func TestAppendFetchRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m := outbox.Message{
		ID:        "m1",
		TaskID:    "t1",
		EventType: "email",
		Payload:   []byte(`{"to":"a@b.c"}`),
	}
	if err := s.Append(ctx, m); err != nil {
		t.Fatal(err)
	}
	// ID 重复 → ErrDuplicateID（哨兵判别测试）。
	if err := s.Append(ctx, m); !errors.Is(err, outbox.ErrDuplicateID) {
		t.Fatalf("重复 Append 必须 ErrDuplicateID: %v", err)
	}
	if err := s.Append(ctx, outbox.Message{ID: "m2", EventType: "sms"}); err != nil {
		t.Fatal(err)
	}
	pending, err := s.FetchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending = %d", len(pending))
	}
	// 文件命名契约：<msgID>.msg.json。
	if _, err := os.Stat(filepath.Join(dir, "m1.msg.json")); err != nil {
		t.Fatal(err)
	}
	// Append 规范化：零值 Status 置 pending；重放安全信息保留。
	if pending[0].Status != outbox.StatusPending {
		t.Fatalf("status = %q", pending[0].Status)
	}
}

func TestNewCleansTmpResidue(t *testing.T) {
	dir := t.TempDir()
	residue := filepath.Join(dir, "m1.msg.json.tmp")
	if err := os.WriteFile(residue, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(residue); !os.IsNotExist(err) {
		t.Fatalf("构造时应清理残留 tmp: %v", err)
	}
}

func TestLifecycleTransitions(t *testing.T) {
	dir := t.TempDir()
	s, _ := New(dir)
	ctx := context.Background()
	_ = s.Append(ctx, outbox.Message{ID: "m1", EventType: "e"})

	// UpdateStatus(processing)。
	if err := s.UpdateStatus(ctx, "m1", outbox.StatusProcessing, 0, ""); err != nil {
		t.Fatal(err)
	}
	pending, _ := s.FetchPending(ctx)
	if len(pending) != 1 || pending[0].Status != outbox.StatusProcessing {
		t.Fatalf("processing 必须重取: %v", pending)
	}
	// MarkProcessed 写入 DeliveredAt。
	if err := s.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.FetchPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("completed 出队: %v", pending)
	}
	// 幂等：不存在的消息视为成功。
	if err := s.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkFailed(ctx, "ghost", "x"); err != nil {
		t.Fatal(err)
	}
	// UpdateStatus 不存在 → error。
	if err := s.UpdateStatus(ctx, "ghost", outbox.StatusPending, 1, "x"); err == nil {
		t.Fatal("UpdateStatus 不存在必须报错")
	}
	// MarkFailed 记录原因。
	_ = s.Append(ctx, outbox.Message{ID: "m2", EventType: "e"})
	if err := s.MarkFailed(ctx, "m2", "handler exploded"); err != nil {
		t.Fatal(err)
	}
	pending, _ = s.FetchPending(ctx)
	if len(pending) != 0 {
		t.Fatalf("failed 出队: %v", pending)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "m2.msg.json"))
	if !strings.Contains(string(data), "handler exploded") {
		t.Fatalf("MarkFailed 必须持久化原因: %s", data)
	}
}

// taskID/消息 ID 必须是文件系统安全字符串。
func TestAppendValidatesID(t *testing.T) {
	s, _ := New(t.TempDir())
	for _, bad := range []string{"", "..", "a/b", "a\\b", "a:b"} {
		if err := s.Append(context.Background(), outbox.Message{ID: bad, EventType: "e"}); err == nil {
			t.Errorf("Append(%q) 应拒绝", bad)
		}
	}
}

// 消息值隔离：文件内容为快照，修改返回值不影响存储。
func TestFetchReturnsCopies(t *testing.T) {
	s, _ := New(t.TempDir())
	ctx := context.Background()
	_ = s.Append(ctx, outbox.Message{ID: "m1", EventType: "e", Payload: []byte(`{"a":1}`)})
	pending, _ := s.FetchPending(ctx)
	pending[0].ID = "HACK"
	pending[0].Payload[0] = 'X'
	again, _ := s.FetchPending(ctx)
	if again[0].ID != "m1" || again[0].Payload[0] != '{' {
		t.Fatal("存储状态被外部修改污染")
	}
}

// FIFO：CreatedAt 升序。
func TestFetchPendingFIFO(t *testing.T) {
	s, _ := New(t.TempDir())
	ctx := context.Background()
	ids := []string{"m3", "m1", "m2"}
	for i, id := range ids {
		m := outbox.Message{ID: id, EventType: "e"}
		// 通过显式 CreatedAt 控制顺序（normalize 只补零值）。
		m.CreatedAt = timeAdd(i)
		_ = s.Append(ctx, m)
	}
	pending, err := s.FetchPending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pending[0].ID != "m3" || pending[1].ID != "m1" || pending[2].ID != "m2" {
		t.Fatalf("FIFO 顺序错误: %v", pending)
	}
}
