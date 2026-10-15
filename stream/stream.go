// Package stream 提供批次管道原语：「取一批 → 写一批 → 落 checkpoint 位置」。
//
// Pipeline 是引擎骨架：把一对 Source/Sink 组合为 engine.Engine，LSN 按
// 补偿侧配对规则计算（见 Pipeline.Execute 文档）。库内附带两个可复用文件
// 组件：ChunkedSink（分块文件汇）与 OffsetSource（偏移行源）。
//
// 补偿侧配对不变量（正确性根基）：
//  1. 只有作为物理补偿目标的一侧才允许实现 Positioner，且该侧必须同时
//     实现 engine.Aligner。
//  2. Sink 与 Source 不得同时实现 Positioner。
//  3. 两侧都无 Aligner 时，回退「累计已处理项数」作为 LSN 才是合法的。
//
// 违反配对规则的管道在 Execute / Align 时返回错误（惰性校验）。
//
// 并发边界：Pipeline 及其组件在 Run 的 goroutine 内同步执行，
// 非并发安全（单任务单执行器前提，设计 §2.4）。
package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
)

// Source 产出下一批。
type Source[Item any] interface {
	// Next 产出下一批。最后一批允许与 io.EOF 同次返回（items 非空 + io.EOF），
	// 管道先处理本批再终止；源彻底耗尽后返回 (nil, io.EOF)。
	Next(ctx context.Context, batchSize int) ([]Item, error)
}

// Sink 写入一批。必须幂等——崩溃恢复会重放同一批。
type Sink[Item any] interface {
	Write(ctx context.Context, items []Item) error
}

// Positioner 报告已提交物理边界（Write 之后调用）。
// 只有作为物理补偿目标的一侧才允许实现，且必须同时实现 engine.Aligner。
type Positioner interface {
	Position() int64
}

// Sizer 报告总量估计（进度）。无 Sizer 时进度可为 0，不得伪造。
type Sizer interface {
	Size() int64
}

// Stater 是源/汇自身的持久化状态（分页游标、分块进度、字节偏移）。
type Stater interface {
	// MarshalState 序列化自身状态（由管道写入 Payload 的 pipeline 命名空间）。
	MarshalState() (json.RawMessage, error)
	// UnmarshalState 必须把源/汇完全回退到上次已提交状态（全量还原，非增量）
	// ——进程内重试复用同一对象，增量还原会导致重放重复。
	// data 为 nil 表示回退到初始状态（任务尚无已提交的管道状态）。
	UnmarshalState(data json.RawMessage) error
}

// Finalizer 在源耗尽后收尾（如分块原子合并）。不得删除重放输入。
type Finalizer interface {
	Finalize(ctx context.Context) error
}

const defaultBatchSize = 1000

// Option 配置 Pipeline。
type Option[Item any] func(*Pipeline[Item])

// WithBatchSize 设置批大小（默认 1000）；n < 1 时 panic。
func WithBatchSize[Item any](n int) Option[Item] {
	if n < 1 {
		panic(fmt.Sprintf("stream: WithBatchSize must be >= 1, got %d", n))
	}
	return func(p *Pipeline[Item]) { p.batchSize = n }
}

// Pipeline 把一对 Source/Sink 组合为「取一批 → 写一批 → 落 checkpoint」的
// 管道骨架，经 AsEngine 包装为 engine.Engine。
type Pipeline[Item any] struct {
	src       Source[Item]
	sink      Sink[Item]
	batchSize int
}

// New 创建管道。
func New[Item any](src Source[Item], sink Sink[Item], opts ...Option[Item]) *Pipeline[Item] {
	p := &Pipeline[Item]{src: src, sink: sink, batchSize: defaultBatchSize}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// pipelineNS 是 Payload 的 pipeline 命名空间结构。
// count 由 Pipeline 自身维护：无 Positioner 的管道以它作为 LSN。
type pipelineNS struct {
	Count int64           `json:"count"`
	Src   json.RawMessage `json:"src,omitempty"`
	Sink  json.RawMessage `json:"sink,omitempty"`
}

// Execute 的引擎骨架语义（设计 §5.3）：
//
//	PhasePending → 置 PhaseRunning，返回 0（零偏移，无进度）；
//	否则：从 Payload 的 pipeline 命名空间还原源/汇 Stater 状态与 count，
//	src.Next(batchSize) → sink.Write(items) → 持久化 Stater 状态与递增后的
//	count → 按配对规则计算 LSN 返回。io.EOF（含最后一批同次返回）时先处理
//	本批，再经 Finalizer 收尾，置 PhaseCompleted。
//
// 每次 Execute 都执行全量还原——这是进程内重试不重复重放的机制依托
// （引擎框架在重试前回滚 Payload，本方法据其还原组件状态）。
func (p *Pipeline[Item]) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	if err := p.validatePairing(); err != nil {
		return 0, err
	}
	count, err := p.restore(s)
	if err != nil {
		return 0, err
	}
	items, err := p.src.Next(ctx, p.batchSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if len(items) > 0 {
		if werr := p.sink.Write(ctx, items); werr != nil {
			return 0, werr
		}
		count += int64(len(items))
	}
	if errors.Is(err, io.EOF) {
		if fin, ok := p.sink.(Finalizer); ok {
			if ferr := fin.Finalize(ctx); ferr != nil {
				return 0, ferr
			}
		}
		s.Phase = state.PhaseCompleted
	}
	if perr := p.persist(s, count); perr != nil {
		return 0, perr
	}
	return p.committedLSN(count), nil
}

// 每步成功 Write 后的 LSN：
//
//	LSN = Sink.Position()      // 汇是补偿侧（如输出文件字节）
//	否则 = Source.Position()   // 源是补偿侧（如源文件字节偏移）
//	否则 = 累计已处理项数       // 两侧皆无物理位置（纯变换 / 截断重放型管道）
func (p *Pipeline[Item]) committedLSN(count int64) state.LSN {
	if pos, ok := p.sink.(Positioner); ok {
		return pos.Position()
	}
	if pos, ok := p.src.(Positioner); ok {
		return pos.Position()
	}
	return count
}

// validatePairing 惰性校验补偿侧配对不变量。
func (p *Pipeline[Item]) validatePairing() error {
	_, srcPos := p.src.(Positioner)
	_, sinkPos := p.sink.(Positioner)
	if srcPos && sinkPos {
		return errors.New("stream: src and sink must not both implement Positioner")
	}
	if srcPos {
		if _, ok := p.src.(engine.Aligner); !ok {
			return errors.New("stream: src implements Positioner but not engine.Aligner")
		}
	}
	if sinkPos {
		if _, ok := p.sink.(engine.Aligner); !ok {
			return errors.New("stream: sink implements Positioner but not engine.Aligner")
		}
	}
	return nil
}

// emptyState 判定 Stater 数据是否表示「无已提交状态」（空或 JSON null），
// 此时组件应回退到初始状态。
func emptyState(data json.RawMessage) bool {
	return len(data) == 0 || string(data) == "null"
}

// restore 从 Payload 的 pipeline 命名空间全量还原组件状态，返回已提交 count。
// Payload 缺失 pipeline 命名空间（或整个 Payload 为空）视作全新状态，
// 组件回退到初始状态；Payload 非法则返回错误（解析失败禁止静默返回空结果）。
func (p *Pipeline[Item]) restore(s *state.BaseTaskState) (int64, error) {
	var ns pipelineNS
	if len(s.Payload) > 0 {
		var top map[string]json.RawMessage
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			return 0, fmt.Errorf("stream: parse task payload: %w", err)
		}
		if raw, ok := top["pipeline"]; ok && !emptyState(raw) {
			if err := json.Unmarshal(raw, &ns); err != nil {
				return 0, fmt.Errorf("stream: parse pipeline state: %w", err)
			}
			if st, ok := p.src.(Stater); ok {
				if err := st.UnmarshalState(ns.Src); err != nil {
					return 0, fmt.Errorf("stream: restore source: %w", err)
				}
			}
			if st, ok := p.sink.(Stater); ok {
				if err := st.UnmarshalState(ns.Sink); err != nil {
					return 0, fmt.Errorf("stream: restore sink: %w", err)
				}
			}
			return ns.Count, nil
		}
	}
	// 全新状态：显式重置，防止失败尝试遗留的内存状态泄漏进重放。
	if st, ok := p.src.(Stater); ok {
		if err := st.UnmarshalState(nil); err != nil {
			return 0, fmt.Errorf("stream: reset source: %w", err)
		}
	}
	if st, ok := p.sink.(Stater); ok {
		if err := st.UnmarshalState(nil); err != nil {
			return 0, fmt.Errorf("stream: reset sink: %w", err)
		}
	}
	return 0, nil
}

// persist 把组件状态与 count 写入 Payload 的 pipeline 命名空间，
// 保留其他命名空间（outbox 等）不动。
func (p *Pipeline[Item]) persist(s *state.BaseTaskState, count int64) error {
	top := map[string]json.RawMessage{}
	if !emptyState(s.Payload) {
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			return fmt.Errorf("stream: parse task payload: %w", err)
		}
		if top == nil {
			top = map[string]json.RawMessage{}
		}
	}
	ns := pipelineNS{Count: count}
	if st, ok := p.src.(Stater); ok {
		raw, err := st.MarshalState()
		if err != nil {
			return fmt.Errorf("stream: marshal source state: %w", err)
		}
		ns.Src = raw
	}
	if st, ok := p.sink.(Stater); ok {
		raw, err := st.MarshalState()
		if err != nil {
			return fmt.Errorf("stream: marshal sink state: %w", err)
		}
		ns.Sink = raw
	}
	raw, err := json.Marshal(ns)
	if err != nil {
		return fmt.Errorf("stream: marshal pipeline state: %w", err)
	}
	top["pipeline"] = raw
	out, err := json.Marshal(top)
	if err != nil {
		return fmt.Errorf("stream: marshal task payload: %w", err)
	}
	s.Payload = out
	return nil
}

// progress 按 position/size 估算进度（有 Positioner + Sizer 时），否则 0。
func (p *Pipeline[Item]) progress() int {
	var pos, size int64
	if pp, ok := p.sink.(Positioner); ok {
		pos = pp.Position()
	} else if pp, ok := p.src.(Positioner); ok {
		pos = pp.Position()
	}
	if sz, ok := p.src.(Sizer); ok {
		size = sz.Size()
	} else if sz, ok := p.sink.(Sizer); ok {
		size = sz.Size()
	}
	if size <= 0 || pos < 0 {
		return 0
	}
	v := pos * 100 / size
	if v < 0 {
		return 0
	}
	if v > 100 {
		return 100
	}
	return int(v)
}

// align 把物理系统对齐到 targetLSN——委托给实现了 engine.Aligner 的一侧。
func (p *Pipeline[Item]) align(ctx context.Context, targetLSN state.LSN) error {
	if err := p.validatePairing(); err != nil {
		return err
	}
	if al, ok := p.src.(engine.Aligner); ok {
		return al.Align(ctx, targetLSN)
	}
	if al, ok := p.sink.(engine.Aligner); ok {
		return al.Align(ctx, targetLSN)
	}
	return nil
}

// AsEngine 以指定 TaskType 包装为 engine.Engine——适配引擎免写转发样板；
// 需要追加业务逻辑（如行编码）时仍可内嵌 Pipeline 自行实现。
//
// 包装按组件能力实现可选接口：任一侧实现 engine.Aligner 时包装实现
// Aligner；有 Positioner + Sizer 时实现 Progressor；无该能力的引擎不实现，
// 不留空方法。
func (p *Pipeline[Item]) AsEngine(taskType string) engine.Engine {
	base := &pipelineEngine[Item]{p: p, taskType: taskType}
	_, srcAl := p.src.(engine.Aligner)
	_, sinkAl := p.sink.(engine.Aligner)
	hasAlign := srcAl || sinkAl
	// Progress 需要 Positioner 与 Sizer 同时可用。
	var pos, size bool
	_, pos = p.sink.(Positioner)
	if !pos {
		_, pos = p.src.(Positioner)
	}
	_, size = p.src.(Sizer)
	if !size {
		_, size = p.sink.(Sizer)
	}
	hasProgress := pos && size
	switch {
	case hasAlign && hasProgress:
		return &alignProgressEngine[Item]{base}
	case hasAlign:
		return &alignEngine[Item]{base}
	case hasProgress:
		return &progressEngine[Item]{base}
	default:
		return base
	}
}

type pipelineEngine[Item any] struct {
	p        *Pipeline[Item]
	taskType string
}

func (e *pipelineEngine[Item]) TaskType() string        { return e.taskType }
func (e *pipelineEngine[Item]) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }
func (e *pipelineEngine[Item]) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	return e.p.Execute(ctx, s)
}

type alignEngine[Item any] struct{ *pipelineEngine[Item] }

func (e *alignEngine[Item]) Align(ctx context.Context, targetLSN state.LSN) error {
	return e.p.align(ctx, targetLSN)
}

type progressEngine[Item any] struct{ *pipelineEngine[Item] }

func (e *progressEngine[Item]) Progress(s state.BaseTaskState) int { return e.p.progress() }

type alignProgressEngine[Item any] struct{ *pipelineEngine[Item] }

func (e *alignProgressEngine[Item]) Align(ctx context.Context, targetLSN state.LSN) error {
	return e.p.align(ctx, targetLSN)
}

func (e *alignProgressEngine[Item]) Progress(s state.BaseTaskState) int { return e.p.progress() }
