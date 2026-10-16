// 示例：分页导出断点续传（设计 §6.1）。
//
// 三幕结构：
//  1. 35 行业务数据分页导出为 JSONL，第 3 页（page=2）拉取失败——Run
//     返回 error 不落盘，任务停留于最后 checkpoint（前 20 行）。
//  2. 反例：同 taskID 换产物路径重跑 → 绑定指纹拒绝恢复（须在世系未
//     完成时——终态 Run 直接返回、不进 Execute，无从校验）。
//  3. 故障解除后重跑同一世系：截断重放保证最终文件恰好包含全部 35 行
//     各一次；completed checkpoint 落盘后调用 Cleanup 清理分块（ADR-3）。
//
// 注意：幕 1 与幕 3 复用同一引擎与 Task——「同源、故障解除」（仅瞬态
// 标志复位），不是同 id 换源；恢复只依赖 checkpoint，同进程复用与跨
// 进程重建等价（源内容身份无法机制化校验，属契约，ADR-10）。
//
// 运行：go run ./export
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jninng/waypoint/datapipeline"
	"github.com/jninng/waypoint/datapipeline/export"
	"github.com/jninng/waypoint/state"
	"github.com/jninng/waypoint/waypoint"
)

const (
	totalRows  = 35
	pageSize   = 10
	poisonPage = 2
)

// sliceSource 静态分页数据源；poisoned 时第 poisonPage 页永久失败
// （模拟上游故障）。
type sliceSource struct {
	rows     []datapipeline.Row
	poisoned bool
}

func (s *sliceSource) FetchPage(ctx context.Context, page, pageSize int) ([]datapipeline.Row, error) {
	if s.poisoned && page == poisonPage {
		return nil, errors.New("page 2: upstream unavailable")
	}
	start := page * pageSize
	if start >= len(s.rows) {
		return nil, nil // 空页：适配层视为源耗尽
	}
	end := min(start+pageSize, len(s.rows))
	return s.rows[start:end], nil
}

func makeRows(n int) []datapipeline.Row {
	rows := make([]datapipeline.Row, n)
	for i := range rows {
		rows[i] = datapipeline.Row{"id": i, "name": fmt.Sprintf("order-%03d", i)}
	}
	return rows
}

func main() {
	dir, err := os.MkdirTemp("", "waypoint-export-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	stateDir := filepath.Join(dir, "states")
	out := filepath.Join(dir, "orders.jsonl")
	rows := makeRows(totalRows)
	ctx := context.Background()

	// 幕一：page 2 拉取失败 → 可重试错误，不落盘。
	// 一个引擎 + 一个 Task 贯穿全例：恢复只依赖 checkpoint（组件每次
	// Execute 全量还原），同进程复用与跨进程重建等价——跨进程时以相同
	// 三元组重建 Task 即可。
	src := &sliceSource{rows: rows, poisoned: true}
	eng := export.New(src, out, export.WithPageSize(pageSize))
	task := waypoint.NewTask(stateDir, eng, "export-42")

	if _, err := task.Run(ctx); err != nil {
		fmt.Printf("第一次 Run 失败: %v\n", err)
	}
	printCheckpoint(stateDir, "export-42")

	// 幕二（反例）：同 id 换产物路径 → 绑定不同，必须另建引擎与 Task →
	// 绑定指纹拒绝恢复（须在世系未完成时构造——终态 Run 不进 Execute）。
	engBad := export.New(&sliceSource{rows: rows}, filepath.Join(dir, "other.jsonl"), export.WithPageSize(pageSize))
	if _, err := waypoint.NewTask(stateDir, engBad, "export-42").Run(ctx); err != nil {
		fmt.Printf("换产物路径重跑被拒绝: %v\n", err)
	}

	// 幕三：故障解除（同一数据源对象，仅瞬态标志复位）→ 同一 Task 重跑
	// 即续传至完成。
	src.poisoned = false
	final, err := task.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("第二次 Run 完成: phase=%s checkpoint_lsn=%d\n", final.Phase, final.CheckpointLSN)

	// completed checkpoint 落盘之后，才允许清理分块（协议顺序，ADR-3）。
	if err := eng.Cleanup(); err != nil {
		log.Fatal(err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	fmt.Printf("最终文件 %d 行: 首行=%s 末行=%s\n", len(lines), lines[0], lines[len(lines)-1])
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
	fmt.Printf("  checkpoint: phase=%s checkpoint_lsn=%d（已提交行数）\n", s.Phase, s.CheckpointLSN)
}
