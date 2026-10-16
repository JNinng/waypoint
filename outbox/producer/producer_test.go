package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/state"
)

// memRepo 与 engine 包测试同构（各测试包自持，不进生产导出面）。
type memRepo struct {
	m map[string][]byte
}

func newMemRepo() *memRepo { return &memRepo{m: map[string][]byte{}} }

func (r *memRepo) Load(_ context.Context, taskID string) ([]byte, error) {
	data, ok := r.m[taskID]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), data...), nil
}

func (r *memRepo) Save(_ context.Context, taskID string, st []byte) error {
	r.m[taskID] = append([]byte(nil), st...)
	return nil
}

func (r *memRepo) Delete(_ context.Context, taskID string) error {
	delete(r.m, taskID)
	return nil
}

// intSource 是切片型记录源：按 after 定位本批；末批与 io.EOF 同次返回。
type intSource struct{ n int }

func (s *intSource) Next(_ context.Context, after int64, batchSize int) ([]int, error) {
	start := int(after)
	if start >= s.n {
		return nil, io.EOF
	}
	end := min(start+batchSize, s.n)
	items := make([]int, 0, end-start)
	for i := start; i < end; i++ {
		items = append(items, i)
	}
	if end >= s.n {
		return items, io.EOF
	}
	return items, nil
}

func plainDerive(_ context.Context, taskID string, r int) (outbox.Message, error) {
	return outbox.Message{ID: fmt.Sprintf("m%02d", r), TaskID: taskID, EventType: "e"}, nil
}

func repoIntents(t *testing.T, repo *memRepo, taskID string) []outbox.Message {
	t.Helper()
	data, err := repo.Load(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if len(s.Payload) > 0 {
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			t.Fatal(err)
		}
	}
	var msgs []outbox.Message
	if raw := top["outbox"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &msgs); err != nil {
			t.Fatal(err)
		}
	}
	return msgs
}

// ADR-12：全流程——引导、分批、末批同次 EOF 收尾、意图落盘与对齐。
func TestProducerFullRun(t *testing.T) {
	eng := New[int]("notify", &intSource{n: 5}, plainDerive, WithBatchSize[int](2))
	repo := newMemRepo()
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted || final.CheckpointLSN != 5 {
		t.Fatalf("终态/LSN 不符: phase=%s lsn=%d", final.Phase, final.CheckpointLSN)
	}
	msgs := repoIntents(t, repo, "t1")
	if len(msgs) != 5 {
		t.Fatalf("应落盘 5 条意图: %d", len(msgs))
	}

	// RunWithOutbox 对齐意图到 Store（§5.6 恢复契约）。
	store := outbox.NewInMemoryStore()
	if _, err := outbox.RunWithOutbox(context.Background(), repo, eng, "t1", store); err != nil {
		t.Fatal(err)
	}
	pending, err := store.FetchPending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 5 {
		t.Fatalf("应对齐 5 条意图: %d", len(pending))
	}
}

// 派生失败 → 可重试 error 不落盘；重放续收：同实例在册意图幂等消化、
// 全新实例经 Restore 还原已落盘意图，均不得丢失或重复。
func TestProducerReplayCollectsOnce(t *testing.T) {
	healed := false
	derive := func(_ context.Context, taskID string, r int) (outbox.Message, error) {
		if !healed && r == 3 { // 第 2 批的第 2 条：r2 已收集在册后失败
			return outbox.Message{}, errors.New("enrich timeout")
		}
		return plainDerive(context.Background(), taskID, r)
	}
	eng := New[int]("notify", &intSource{n: 5}, derive, WithBatchSize[int](2))
	repo := newMemRepo()
	_, err := engine.Run(context.Background(), repo, eng, "t1")
	if err == nil {
		t.Fatal("期望派生失败中断")
	}
	if !strings.Contains(err.Error(), "record 3") {
		t.Fatalf("错误应可定位到记录: %v", err)
	}
	// checkpoint：第一批 2 条已落盘；r2 的意图只在本进程收集器内存。
	if msgs := repoIntents(t, repo, "t1"); len(msgs) != 2 {
		t.Fatalf("失败后已落盘意图应为 2 条: %d", len(msgs))
	}

	// 故障解除：同实例重跑——重放收集 r2 在册幂等消化，最终恰 5 条不重复。
	healed = true
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.CheckpointLSN != 5 {
		t.Fatalf("续收后 LSN=5: %d", final.CheckpointLSN)
	}
	ids := map[string]int{}
	for _, m := range repoIntents(t, repo, "t1") {
		ids[m.ID]++
	}
	if len(ids) != 5 {
		t.Fatalf("重放收集不得丢失或重复意图: %v", ids)
	}

	// 跨进程重建：全新引擎（全新收集器）经 Restore 还原已落盘意图后续收。
	fresh := New[int]("notify", &intSource{n: 5}, plainDerive, WithBatchSize[int](2))
	if _, err := engine.Run(context.Background(), repo, fresh, "t1"); err != nil {
		t.Fatal(err)
	}
	ids = map[string]int{}
	for _, m := range repoIntents(t, repo, "t1") {
		ids[m.ID]++
	}
	if len(ids) != 5 {
		t.Fatalf("全新实例还原续收不得丢失意图: %v", ids)
	}
}

// 空源：直接 completed，零意图，LSN 0。
func TestProducerEmptySource(t *testing.T) {
	eng := New[int]("notify", &intSource{n: 0}, plainDerive)
	repo := newMemRepo()
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted || final.CheckpointLSN != 0 {
		t.Fatalf("空源应 completed/LSN 0: phase=%s lsn=%d", final.Phase, final.CheckpointLSN)
	}
	if msgs := repoIntents(t, repo, "t1"); len(msgs) != 0 {
		t.Fatalf("空源不得产生意图: %d", len(msgs))
	}
}

// WithBatchSize 负值参数构造时 panic。
func TestWithBatchSizePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("n < 1 必须 panic")
		}
	}()
	WithBatchSize[int](0)
}
