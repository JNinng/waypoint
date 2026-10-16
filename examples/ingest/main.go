// 示例：JSONL 导入偏移恢复（设计 §6.2）。
//
// 12 行 JSONL 按批（5 行/批）导入幂等目标（UPSERT）。第 2 批「写入成功
// 但提交确认丢失」（模拟超时）→ 首次 Run 失败，checkpoint 停在源文件
// 字节偏移（含行分隔符累计）。重跑从该偏移恢复、重放第 2、3 批；
// 幂等目标去重后 12 行各入库一次——DataTarget 契约（重放安全）消解
// at-least-once 重放。补偿侧是源：恢复时校验源文件大小 ≥ LSN。
// 源路径参与绑定指纹：换 srcPath 重跑同 taskID 会被拒绝（ADR-10）。
//
// 运行：go run ./ingest
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jninng/waypoint/datapipeline"
	"github.com/jninng/waypoint/datapipeline/ingest"
	"github.com/jninng/waypoint/state"
	"github.com/jninng/waypoint/waypoint"
)

const batchSize = 5

// memTarget 幂等写入目标：按 id UPSERT。
// failBatch2 时第 2 次 WriteBatch 先落库、再返回错误——模拟
// 「写成功但提交确认丢失」；重放安全的目标必须消化这种场景。
type memTarget struct {
	upserted    map[string]datapipeline.Row
	order       []string
	rowsApplied int
	calls       int
	failBatch2  bool
}

func newMemTarget(failBatch2 bool) *memTarget {
	return &memTarget{upserted: map[string]datapipeline.Row{}, failBatch2: failBatch2}
}

func (t *memTarget) WriteBatch(ctx context.Context, rows []datapipeline.Row) (int64, error) {
	t.calls++
	t.rowsApplied += len(rows)
	var inserted int64
	for _, r := range rows {
		id, _ := r["id"].(string)
		if _, seen := t.upserted[id]; !seen {
			t.order = append(t.order, id)
			inserted++
		}
		t.upserted[id] = r
	}
	if t.failBatch2 && t.calls == 2 {
		return inserted, errors.New("commit timeout after write")
	}
	return inserted, nil
}

func writeSource(path string) (int, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for i := 0; i < 12; i++ {
		if err := enc.Encode(map[string]any{"id": fmt.Sprintf("r%02d", i)}); err != nil {
			return 0, err
		}
	}
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return int(info.Size()), nil
}

func main() {
	dir, err := os.MkdirTemp("", "waypoint-ingest-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	stateDir := filepath.Join(dir, "states")
	srcPath := filepath.Join(dir, "events.jsonl")
	srcBytes, err := writeSource(srcPath)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	// 第一次运行：第 2 批写入后「提交确认丢失」→ Run 失败。
	// 一个引擎 + 一个 Task 贯穿全例（恢复只依赖 checkpoint，组件每次
	// Execute 全量还原）；源路径参与绑定指纹，换 srcPath 重跑同 id 会被拒绝。
	target := newMemTarget(true)
	eng := ingest.New(srcPath, target, ingest.WithBatchSize(batchSize))
	task := waypoint.NewTask(stateDir, eng, "ingest-42")

	if _, err := task.Run(ctx); err != nil {
		fmt.Printf("第一次 Run 失败: %v\n", err)
	}
	printCheckpoint(stateDir, "ingest-42")

	// 重跑：故障解除（同一目标对象，注入标志复位；已落库的行还在），
	// 同一 Task 重跑 → 从源字节偏移续传。
	target.failBatch2 = false
	final, err := task.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("第二次 Run 完成: phase=%s checkpoint_lsn=%d（源文件字节数 %d）\n",
		final.Phase, final.CheckpointLSN, srcBytes)
	fmt.Printf("目标入库: 唯一行=%d（源 12 行），累计写入=%d 行 / %d 批——重放被幂等目标去重\n",
		len(target.upserted), target.rowsApplied, target.calls)
}

// printCheckpoint 直接读状态文件（Task 隐藏了 repo，演示用直读）。
func printCheckpoint(stateDir, taskID string) {
	data, err := os.ReadFile(filepath.Join(stateDir, taskID+".state"))
	if err != nil {
		log.Fatal(err)
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  checkpoint: phase=%s checkpoint_lsn=%d（源文件字节偏移）\n", s.Phase, s.CheckpointLSN)
}
