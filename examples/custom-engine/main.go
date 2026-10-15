// 示例：最小自定义引擎与断点恢复（设计 §5.2 / ADR-1）。
//
// 场景一：三步「数据块搬运」任务，第 2 块首次执行失败（可重试错误）——
// Run 返回 error 且不落盘，任务停留于最后已提交 checkpoint；故障消除后
// 重跑（全新引擎实例 + 同一 taskID），从 checkpoint 恢复直至完成。
//
// 场景二：优雅失败——引擎在 Execute 内置 PhaseFailed 并返回 nil，
// 这是唯一落盘的失败路径（框架不兜底写终态）。
//
// 运行：go run ./custom-engine
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
	"github.com/jninng/waypoint/state"
)

const totalChunks = 3

// chunkEngine 每步搬运一个数据块，LSN = 已完成块数。
// staged 模拟外部系统的已落库集合；副作用按块幂等，重放不累积
// （Execute 重入契约，设计 §5.2）。
type chunkEngine struct {
	staged    map[int]bool
	failChunk int // >= 0 时该块的执行永久失败（模拟故障）
}

func (e *chunkEngine) TaskType() string        { return "chunk-move" }
func (e *chunkEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }

func (e *chunkEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	next := int(s.CheckpointLSN) // 已提交块数即下一个待处理块
	if next >= totalChunks {
		s.Phase = state.PhaseCompleted
		return s.CheckpointLSN, nil
	}
	if next == e.failChunk {
		return 0, fmt.Errorf("chunk %d: upstream unavailable", next)
	}
	e.staged[next] = true // 物理副作用（幂等）
	fmt.Printf("  已搬运数据块 %d\n", next)
	if next+1 == totalChunks {
		s.Phase = state.PhaseCompleted
	}
	return int64(next + 1), nil
}

// doomedEngine 第二步起显式判定任务失败：置 PhaseFailed 并返回 nil。
type doomedEngine struct{}

func (doomedEngine) TaskType() string        { return "validate" }
func (doomedEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (doomedEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	s.Phase = state.PhaseFailed // 优雅失败：唯一落盘的失败路径
	s.Message = "源数据校验失败：字段 price 存在非法值"
	return 0, nil
}

func main() {
	dir, err := os.MkdirTemp("", "waypoint-engine-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	repo, err := filestore.New(filepath.Join(dir, "states"))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	fmt.Println("场景一：可重试错误 → 不落盘 → 从 checkpoint 恢复")
	eng := &chunkEngine{staged: map[int]bool{}, failChunk: 1}
	if _, err := engine.Run(ctx, repo, eng, "move-42"); err != nil {
		fmt.Printf("  第一次 Run 失败: %v\n", err)
	}
	printCheckpoint(repo, "move-42")

	// 故障消除：全新引擎实例 + 同一 taskID，外部副作用集合延续。
	resumed := &chunkEngine{staged: eng.staged, failChunk: -1}
	final, err := engine.Run(ctx, repo, resumed, "move-42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  第二次 Run 完成: phase=%s checkpoint_lsn=%d staged=%v\n",
		final.Phase, final.CheckpointLSN, stagedChunks(eng.staged))

	fmt.Println("场景二：优雅失败（引擎显式判定，唯一落盘失败路径）")
	dfinal, err := engine.Run(ctx, repo, doomedEngine{}, "check-7")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  phase=%s message=%q\n", dfinal.Phase, dfinal.Message)
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
	fmt.Printf("  checkpoint: phase=%s checkpoint_lsn=%d\n", s.Phase, s.CheckpointLSN)
}

func stagedChunks(m map[int]bool) []int {
	out := []int{}
	for i := 0; i < totalChunks; i++ {
		if m[i] {
			out = append(out, i)
		}
	}
	return out
}
