// Package producer 把 outbox 生产引擎的通用骨架收敛为组件：引导步、已落盘
// 意图还原（ADR-11）、按批推进、意图收集与合并、耗尽收尾全部固定在
// Execute 内，业务方只需提供记录源（Source）与意图派生函数（Derive）——
// 「一步做什么」的本体，编排不再是手抄样板（ADR-12）。
//
// 骨架语义：pending→running 引导步；每业务步 Restore 已落盘意图 → 取一批
// → 逐条派生并幂等收集 → Flush 合并回命名空间；源耗尽置 completed。
// LSN = 已提交记录数（组件统一声明，约束 §8-8）；Phases =
// engine.DefaultPhaseSet；意图随 checkpoint 原子落盘。
//
// 依赖边界：本包组合 engine/outbox/state 为引擎骨架，是「outbox 不依赖
// engine」的 sanctioned 例外之一（另一为 outbox.RunWithOutbox 桥接，ADR-12）。
//
// 并发边界：随引擎在任务 goroutine 内同步执行，无并发。
package producer

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/state"
)

const defaultBatchSize = 1000

// Source 产出待产意图的业务记录。
//
// 契约：
//   - after 是已提交记录数（引擎 LSN）——切片/分页型源据此定位本批起点；
//     源自身无状态，恢复定位由组件依据 checkpoint 完成。
//   - 业务副作用（如把记录标记为已受理）在 Next 内完成——先副作用后意图
//     收集，与 Execute 时序契约一致（设计 §5.2）；副作用必须幂等，重放
//     同一批不累积。
//   - 最后一批允许与 io.EOF 同次返回（items 非空 + io.EOF），组件先收集
//     本批再收尾；彻底耗尽后返回 (nil, io.EOF)。
//   - 同一 taskID 内意图 ID 必须由业务键唯一派生（Collector 对在册意图
//     幂等，重复 ID 的派生会被视作重放跳过）。
type Source[Record any] interface {
	Next(ctx context.Context, after int64, batchSize int) ([]Record, error)
}

// Derive 从一条业务记录派生一条不可逆操作意图；派生失败返回可重试 error
// （不落盘，重放时已收集的意图由 Collector 幂等消化）。
type Derive[Record any] func(ctx context.Context, taskID string, r Record) (outbox.Message, error)

// Option 配置生产引擎。
type Option[Record any] func(*Engine[Record])

// WithBatchSize 设置每批记录数（默认 1000）；n < 1 时 panic。
func WithBatchSize[Record any](n int) Option[Record] {
	if n < 1 {
		panic(fmt.Sprintf("producer: WithBatchSize must be >= 1, got %d", n))
	}
	return func(e *Engine[Record]) { e.batchSize = n }
}

// Engine 是 outbox 生产引擎：骨架编排组件内固定，业务面收缩为
// Source + Derive。零值不可用，经 New 构造。
type Engine[Record any] struct {
	taskType  string
	src       Source[Record]
	derive    Derive[Record]
	batchSize int
	col       outbox.Collector
}

// New 组合「记录源 + 意图派生 + Collector」为 outbox 生产引擎。
func New[Record any](taskType string, src Source[Record], derive Derive[Record], opts ...Option[Record]) *Engine[Record] {
	e := &Engine[Record]{taskType: taskType, src: src, derive: derive, batchSize: defaultBatchSize}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// TaskType 实现 engine.Engine。
func (e *Engine[Record]) TaskType() string { return e.taskType }

// Phases 实现 engine.Engine：默认协议阶段。
func (e *Engine[Record]) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }

// Execute 实现 engine.Engine：组件内固定的生产骨架。
func (e *Engine[Record]) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	// 步首：还原已落盘意图（每实例一次生效）。跳过会导致本步 Flush 以
	// 「只有新意图」的视图覆盖命名空间、丢失已落盘意图（ADR-11）。
	if err := e.col.Restore(s); err != nil {
		return 0, err
	}
	after := s.CheckpointLSN // 已提交记录数即本批起点（LSN = 已提交记录数）
	items, err := e.src.Next(ctx, after, e.batchSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	for i, r := range items {
		msg, derr := e.derive(ctx, s.TaskID, r)
		if derr != nil {
			return 0, fmt.Errorf("producer: derive record %d: %w", after+int64(i), derr)
		}
		// 重放收集中已在册的意图由 Collector 幂等消化（ADR-11）。
		if err := e.col.Add(msg); err != nil {
			return 0, err
		}
	}
	// 步末：收集器全量视图合并进 Payload，随本步 checkpoint 原子落盘。
	if err := e.col.Flush(s); err != nil {
		return 0, err
	}
	if errors.Is(err, io.EOF) {
		s.Phase = state.PhaseCompleted
	}
	return after + int64(len(items)), nil
}
