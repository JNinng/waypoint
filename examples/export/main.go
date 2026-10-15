// 示例：分页导出断点续传（设计 §6.1）。
//
// 35 行业务数据分页导出为 JSONL；第 3 页（page=2）首次拉取失败——
// Run 返回 error 不落盘，任务停留于最后 checkpoint（前 20 行）。
// 重跑：全新引擎从第 3 页继续，截断重放保证最终文件恰好包含全部 35 行
// 各一次；completed checkpoint 落盘后调用 Cleanup 清理分块中间产物
// （协议顺序，ADR-3）。
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
	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
	"github.com/jninng/waypoint/state"
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
	repo, err := filestore.New(filepath.Join(dir, "states"))
	if err != nil {
		log.Fatal(err)
	}
	out := filepath.Join(dir, "orders.jsonl")
	rows := makeRows(totalRows)
	ctx := context.Background()

	// 第一次运行：page 2 拉取失败 → 可重试错误，不落盘。
	eng1 := export.New(&sliceSource{rows: rows, poisoned: true}, out, export.WithPageSize(pageSize))
	if _, err := engine.Run(ctx, repo, eng1, "export-42"); err != nil {
		fmt.Printf("第一次 Run 失败: %v\n", err)
	}
	printCheckpoint(repo, "export-42")

	// 故障消除后重跑：全新引擎实例，同一 taskID 与输出路径。
	eng2 := export.New(&sliceSource{rows: rows}, out, export.WithPageSize(pageSize))
	final, err := engine.Run(ctx, repo, eng2, "export-42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("第二次 Run 完成: phase=%s checkpoint_lsn=%d\n", final.Phase, final.CheckpointLSN)

	// completed checkpoint 落盘之后，才允许清理分块（协议顺序，ADR-3）。
	if err := eng2.Cleanup(); err != nil {
		log.Fatal(err)
	}

	data, err := os.ReadFile(out)
	if err != nil {
		log.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	fmt.Printf("最终文件 %d 行: 首行=%s 末行=%s\n", len(lines), lines[0], lines[len(lines)-1])
}

func printCheckpoint(repo state.StateRepository, taskID string) {
	data, err := repo.Load(context.Background(), taskID)
	if err != nil {
		log.Fatal(err)
	}
	if data == nil {
		fmt.Println("  checkpoint: 无（未落盘）")
		return
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  checkpoint: phase=%s checkpoint_lsn=%d（已提交行数）\n", s.Phase, s.CheckpointLSN)
}
