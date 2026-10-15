package ingest

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
	"github.com/jninng/waypoint/state"
)

// memRepo：内存状态仓库，可注入第 N 次 Save 失败（模拟崩溃窗口）。
type memRepo struct {
	mu     sync.Mutex
	m      map[string][]byte
	saves  int
	failOn int
}

func newMemRepo() *memRepo { return &memRepo{m: map[string][]byte{}} }

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
		return errors.New("simulated crash: write done, checkpoint lost")
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

func (r *memRepo) load(t *testing.T, taskID string) *state.BaseTaskState {
	t.Helper()
	data, err := r.Load(context.Background(), taskID)
	if err != nil || data == nil {
		return nil
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return &s
}

// memTarget 是幂等目标模拟：UPSERT 语义（按 id 去重），记录总写入调用。
type memTarget struct {
	mu       sync.Mutex
	upserted map[string]datapipeline.Row
	order    []string // 首次出现的 id 顺序
	batches  int
}

func newMemTarget() *memTarget { return &memTarget{upserted: map[string]datapipeline.Row{}} }

func (tg *memTarget) WriteBatch(ctx context.Context, rows []datapipeline.Row) (int64, error) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	tg.batches++
	var inserted int64
	for _, r := range rows {
		id := fmt.Sprint(r["id"])
		if _, seen := tg.upserted[id]; !seen {
			tg.order = append(tg.order, id)
			inserted++
		}
		tg.upserted[id] = r
	}
	return inserted, nil
}

func writeJSONL(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func jsonlLines(n int) (string, []string) {
	var sb strings.Builder
	var ids []string
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("r%03d", i)
		line, _ := json.Marshal(map[string]any{"id": id})
		sb.Write(line)
		sb.WriteString("\r\n") // 混合 CRLF：源组件契约必须兼容
		ids = append(ids, id)
	}
	return sb.String(), ids
}

// 设计 §6.2：JSONL 导入偏移恢复——正常全流程。
func TestIngestFullRun(t *testing.T) {
	content, ids := jsonlLines(12)
	src := writeJSONL(t, content)
	repo := newMemRepo()
	target := newMemTarget()
	eng := New(src, target, WithBatchSize(5))

	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	// 导入的补偿侧是源：LSN = 源文件字节偏移。
	if final.CheckpointLSN != int64(len(content)) {
		t.Fatalf("lsn = %d, want %d（文件字节数）", final.CheckpointLSN, len(content))
	}
	if len(target.order) != len(ids) {
		t.Fatalf("入库行数 = %d, want %d", len(target.order), len(ids))
	}
	for i, id := range ids {
		if target.order[i] != id {
			t.Fatalf("顺序错位 at %d: %v", i, target.order)
		}
	}
}

// 崩溃点（设计 §6.2）：目标写后 Save 前——恢复时源偏移回退 + 幂等重放，
// 有效入库不重复。
func TestIngestCrashBeforeCheckpointReplay(t *testing.T) {
	content, ids := jsonlLines(12)
	src := writeJSONL(t, content)
	repo := &memRepo{m: map[string][]byte{}, failOn: 3} // 第 3 次 Save 失败
	target := newMemTarget()

	eng1 := New(src, target, WithBatchSize(5))
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望第 3 次 Save 失败")
	}
	// 幂等重入探针：模拟进程重建（新引擎、同目标、同状态仓库）。
	eng2 := New(src, target, WithBatchSize(5))
	final, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	// UPSERT 去重后：有效入库恰好每行一次。
	if len(target.order) != len(ids) {
		t.Fatalf("重放后有效入库 = %d, want %d（幂等目标去重失效？）", len(target.order), len(ids))
	}
}

// 源被截断 → Align 拒绝恢复（设计 §5.4.2 失败模式）。
func TestIngestAlignRejectsTruncatedSource(t *testing.T) {
	content, _ := jsonlLines(10)
	src := writeJSONL(t, content)
	repo := newMemRepo()
	target := newMemTarget()

	// 先跑一部分：批大小 4 → 完成 2 批后中断（用 failOn 制造停顿）。
	repo2 := &memRepo{m: map[string][]byte{}, failOn: 3}
	eng1 := New(src, target, WithBatchSize(4))
	if _, err := engine.Run(context.Background(), repo2, eng1, "t1"); err == nil {
		t.Fatal("期望中断")
	}
	saved := repo2.load(t, "t1")
	if saved.CheckpointLSN == 0 {
		t.Fatal("应有已提交偏移")
	}
	// 把状态挪到普通仓库（模拟重启后恢复环境）。
	data, _ := repo2.Load(context.Background(), "t1")
	repo.m["t1"] = data

	// 源文件被截断。
	if err := os.WriteFile(src, []byte("{\"id\":\"x\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	eng2 := New(src, newMemTarget(), WithBatchSize(4))
	_, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err == nil || !strings.Contains(err.Error(), "source") {
		t.Fatalf("源截断后必须拒绝恢复: %v", err)
	}
}

// 恢复时 Align 先于 Stater 还原执行（引擎编排契约）。
func TestIngestAlignBeforeStaterRestore(t *testing.T) {
	content, _ := jsonlLines(10)
	src := writeJSONL(t, content)
	repo := newMemRepo()
	target := newMemTarget()
	eng1 := New(src, target, WithBatchSize(4))
	repo.failOn = 2
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望中断")
	}
	repo.failOn = 0

	// fresh 引擎：Run 恢复路径应先 Align（校验文件大小）再 Execute。
	eng2 := New(src, target, WithBatchSize(4))
	if _, ok := any(eng2).(engine.Aligner); !ok {
		t.Fatal("ingest 引擎应实现 Aligner（源是补偿侧）")
	}
	final, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
	if len(target.order) != 10 {
		t.Fatalf("入库 = %d", len(target.order))
	}
}

// 行解码失败由调用方/适配层处理：报错可恢复，不落盘。
func TestIngestDecodeError(t *testing.T) {
	src := writeJSONL(t, "{\"id\":\"a\"}\nnot-json\n")
	repo := newMemRepo()
	eng := New(src, newMemTarget(), WithBatchSize(10))
	_, err := engine.Run(context.Background(), repo, eng, "t1")
	if err == nil || !strings.Contains(err.Error(), "decode line") {
		t.Fatalf("解码失败必须报错: %v", err)
	}
}

// 自定义解码器（如 CSV）。
func TestIngestCustomUnmarshaler(t *testing.T) {
	src := writeJSONL(t, "")
	src = filepath.Join(filepath.Dir(src), "src.csv")
	if err := os.WriteFile(src, []byte("a,b\n1,2\n3,4\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repo := newMemRepo()
	target := newMemTarget()
	eng := New(src, target,
		WithRowUnmarshaler(func(line []byte) (datapipeline.Row, error) {
			parts := strings.Split(string(line), ",")
			return datapipeline.Row{"id": parts[0], "v": parts[1]}, nil
		}),
		WithBatchSize(10))
	final, err := engine.Run(context.Background(), repo, eng, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != "completed" || len(target.order) != 3 {
		t.Fatalf("final = %+v, rows = %v", final, target.order)
	}
}

func TestIngestTaskType(t *testing.T) {
	eng := New(writeJSONL(t, ""), newMemTarget())
	if eng.TaskType() != "ingest" {
		t.Fatalf("task type = %q", eng.TaskType())
	}
}
