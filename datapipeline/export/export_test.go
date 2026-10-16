package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jninng/waypoint/datapipeline"
	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
)

// memSource 是分页数据源模拟：静态行集 + 页拉取计数；
// failPages 中的页失败一次后移除（模拟瞬态故障）。
type memSource struct {
	mu        sync.Mutex
	rows      []datapipeline.Row
	fetches   []int
	failPages map[int]error
}

func (s *memSource) FetchPage(ctx context.Context, page, pageSize int) ([]datapipeline.Row, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetches = append(s.fetches, page)
	if err, ok := s.failPages[page]; ok {
		delete(s.failPages, page)
		return nil, err
	}
	start := page * pageSize
	if start >= len(s.rows) {
		return nil, nil // 空页 → 适配层视为耗尽
	}
	end := min(start+pageSize, len(s.rows))
	return s.rows[start:end], nil
}

func newRows(n int) []datapipeline.Row {
	rows := make([]datapipeline.Row, n)
	for i := range rows {
		rows[i] = datapipeline.Row{"id": i, "name": fmt.Sprintf("row-%03d", i)}
	}
	return rows
}

func expectedLines(rows []datapipeline.Row) string {
	var sb strings.Builder
	for _, r := range rows {
		b, _ := json.Marshal(r)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// memRepo：内存状态仓库，可注入第 N 次 Save 失败（模拟崩溃窗口）。
type memRepo struct {
	mu     sync.Mutex
	m      map[string][]byte
	saves  int
	failOn int
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
	r.saves++
	if r.failOn != 0 && r.saves == r.failOn {
		return errors.New("simulated crash: execute done, checkpoint lost")
	}
	r.m[taskID] = append([]byte(nil), s...)
	return nil
}

func (r *memRepo) Delete(ctx context.Context, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, taskID)
	return nil
}

// 设计 §6.1：分块导出断点续传——正常全流程。
func TestExportFullRun(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "out", "export.jsonl")
	repo := newMemRepo()
	src := &memSource{rows: newRows(25)}
	eng := New(src, out, WithPageSize(10), WithBatchesPerChunk(2))

	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	// 分块导出的 LSN 回退「累计项数」。
	if final.CheckpointLSN != 25 {
		t.Fatalf("lsn = %d, want 25", final.CheckpointLSN)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	// 验收：分块导出断言最终文件逐字节等于源数据。
	if string(got) != expectedLines(src.rows) {
		t.Fatalf("最终文件与源数据不一致:\n got  %d bytes\n want %d bytes", len(got), len(expectedLines(src.rows)))
	}
	// 分块仍保留（Cleanup 前是重放输入）。
	if err := eng.Cleanup(); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Dir(out))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".chunk_") || strings.HasSuffix(e.Name(), ".final.tmp") {
			t.Fatalf("Cleanup 后残留中间产物: %s", e.Name())
		}
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal("Cleanup 不得删除最终文件")
	}
}

// 崩溃点 A（设计 §6.1）：Write 后 Save 前——恢复时截断重放，不重复。
func TestExportCrashBeforeCheckpointReplay(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	rows := newRows(25)
	src := &memSource{rows: rows}
	repo := &memRepo{m: map[string][]byte{}, failOn: 3} // 第 3 次 Save 失败

	eng1 := New(src, out, WithPageSize(10), WithBatchesPerChunk(2))
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望第 3 次 Save 失败导致 Run 报错")
	}

	// 模拟进程重建：全新引擎 + 同一持久化状态。
	eng2 := New(&memSource{rows: rows}, out, WithPageSize(10), WithBatchesPerChunk(2))
	final, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" || final.CheckpointLSN != 25 {
		t.Fatalf("final = %+v", final)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != expectedLines(rows) {
		t.Fatal("崩溃后重放导致最终文件与源数据不一致（截断重放被破坏）")
	}
}

// 崩溃点 B（设计 §6.1）：Finalize 后 completed Save 前——重放 Finalize 幂等。
func TestExportCrashAfterFinalizeReplay(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	rows := newRows(8)
	repo := &memRepo{m: map[string][]byte{}}

	// 找到 completed 那次 Save 的序号：pending→running(1) + 每批一次 +
	// EOF/completed(最后)。页大小 8 → 一批 + completed = 第 3 次。
	repo.failOn = 3
	src := &memSource{rows: rows}
	eng1 := New(src, out, WithPageSize(8))
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望 completed Save 失败")
	}
	// 此刻最终文件可能已 Finalize 产出，但 checkpoint 停留在 running。
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("Finalize 应已产出最终文件: %v", err)
	}

	eng2 := New(&memSource{rows: rows}, out, WithPageSize(8))
	final, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	got, _ := os.ReadFile(out)
	if string(got) != expectedLines(rows) {
		t.Fatal("Finalize 重放后最终文件与源数据不一致")
	}
}

// 瞬态分页失败 + 进程内重试（WithRetry）：页游标回退后重放同一页。
func TestExportTransientPageFailureWithRetry(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	rows := newRows(20)
	src := &memSource{rows: rows, failPages: map[int]error{1: errors.New("transient")}}
	repo := newMemRepo()
	eng := New(src, out, WithPageSize(10))
	final, err := engine.Run(context.Background(), repo, eng, "t1", engine.WithRetry(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if final.CheckpointLSN != 20 {
		t.Fatalf("lsn = %d", final.CheckpointLSN)
	}
	got, _ := os.ReadFile(out)
	if string(got) != expectedLines(rows) {
		t.Fatal("重试重放后最终文件与源数据不一致")
	}
}

// 同一引擎实例跨 Run 复用：全量还原契约保证组件内存状态在每次 Execute
// 从 checkpoint 重建，恢复不依赖实例记忆（examples 单引擎叙事的锚点）。
func TestExportSameEngineReuseAcrossRuns(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	rows := newRows(20)
	src := &memSource{rows: rows, failPages: map[int]error{1: errors.New("page 1 transient")}}
	repo := newMemRepo()
	eng := New(src, out, WithPageSize(10))

	// 第一次 Run：page 1 失败（failPages 自移除 → 瞬态语义）。
	if _, err := engine.Run(context.Background(), repo, eng, "t1"); err == nil {
		t.Fatal("期望中断")
	}
	// 同一引擎实例重跑：从 checkpoint 续传至完成。
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatalf("同引擎复用恢复被拒: %v", err)
	}
	if final.Phase != "completed" || final.CheckpointLSN != 20 {
		t.Fatalf("final = %+v", final)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != expectedLines(rows) {
		t.Fatal("同引擎跨 Run 恢复导致最终文件与源数据不一致")
	}
}

// 空数据源：直接 completed，产出空文件。
func TestExportEmptySource(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	repo := newMemRepo()
	eng := New(&memSource{}, out)
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" || final.CheckpointLSN != 0 {
		t.Fatalf("final = %+v", final)
	}
	got, _ := os.ReadFile(out)
	if len(got) != 0 {
		t.Fatalf("got %q", got)
	}
}

// 行编码失败 → Execute 报错（可重试/可恢复，不落盘）。
func TestExportMarshalError(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	repo := newMemRepo()
	eng := New(&memSource{rows: newRows(1)}, out,
		WithRowMarshaler(func(row datapipeline.Row) ([]byte, error) {
			return nil, errors.New("bad row")
		}))
	_, err := engine.Run(context.Background(), repo, eng, "t1")
	if err == nil || !strings.Contains(err.Error(), "marshal row") {
		t.Fatalf("行编码失败必须报错: %v", err)
	}
}

// TaskType 契约："export"。
func TestExportTaskType(t *testing.T) {
	eng := New(&memSource{}, filepath.Join(t.TempDir(), "out.jsonl"))
	if eng.TaskType() != "export" {
		t.Fatalf("task type = %q", eng.TaskType())
	}
	if got := eng.Phases(); len(got.All) != 4 || !got.IsTerminal("completed") {
		t.Fatalf("Phases 应为 DefaultPhaseSet: %+v", got)
	}
}

// 与文件后端（filestore）组合的集成烟测。
func TestExportWithFileRepository(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "export.jsonl")
	repo, err := filestore.New(filepath.Join(base, "states"))
	if err != nil {
		t.Fatal(err)
	}
	rows := newRows(7)
	eng := New(&memSource{rows: rows}, out, WithPageSize(3), WithBatchesPerChunk(1))
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	got, _ := os.ReadFile(out)
	if string(got) != expectedLines(rows) {
		t.Fatal("文件后端组合下最终文件与源数据不一致")
	}
}

func newMemRepo() *memRepo { return &memRepo{m: map[string][]byte{}} }
