// Package ingest 是导入垂直引擎：组合「stream.OffsetSource + DataTarget
// 适配的幂等 Sink」。TaskType = "ingest"。
//
// 补偿侧是源（偏移校验：Align 校验源文件大小 ≥ LSN），目标是幂等重放
// （UPSERT / 去重——DataTarget 契约）。源侧 Align 先于 Stater 还原执行。
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/jninng/waypoint/datapipeline"
	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/stream"
)

const taskType = "ingest"

// RowUnmarshaler 把原始行字节（不含行尾符）解码为一行数据。
type RowUnmarshaler func(line []byte) (datapipeline.Row, error)

// Engine 组装导入管道。零值不可用，经 New 构造。
// 源侧是补偿侧：Engine 固定实现 engine.Aligner（转发 OffsetSource 的
// 偏移校验）。
type Engine struct {
	engine.Engine // stream.Pipeline.AsEngine("ingest")
	src           *decodeSource
}

// Option 配置导入引擎。
type Option func(*config)

type config struct {
	batchSize      int
	rowUnmarshaler RowUnmarshaler
}

// WithBatchSize 设置批大小（默认 1000）；n < 1 时 panic。
func WithBatchSize(n int) Option {
	if n < 1 {
		panic(fmt.Sprintf("ingest: WithBatchSize must be >= 1, got %d", n))
	}
	return func(c *config) { c.batchSize = n }
}

// WithRowUnmarshaler 自定义行解码（默认 json.Unmarshal）。nil 时 panic。
func WithRowUnmarshaler(f RowUnmarshaler) Option {
	if f == nil {
		panic("ingest: WithRowUnmarshaler must not be nil")
	}
	return func(c *config) { c.rowUnmarshaler = f }
}

// New 创建导入引擎；srcPath 是 JSONL 等文本文件路径。
func New(srcPath string, target datapipeline.DataTarget, opts ...Option) *Engine {
	cfg := config{
		batchSize:      1000,
		rowUnmarshaler: defaultUnmarshaler,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	src := &decodeSource{src: stream.NewOffsetSource(srcPath), unmarshaler: cfg.rowUnmarshaler}
	p := stream.New[datapipeline.Row](src, &targetSink{target: target},
		stream.WithBatchSize[datapipeline.Row](cfg.batchSize))
	return &Engine{Engine: p.AsEngine(taskType), src: src}
}

// Align 实现 engine.Aligner：转发源侧补偿（校验源文件大小 ≥ LSN）。
func (e *Engine) Align(ctx context.Context, targetLSN int64) error {
	return e.src.Align(ctx, targetLSN)
}

func defaultUnmarshaler(line []byte) (datapipeline.Row, error) {
	var row datapipeline.Row
	if err := json.Unmarshal(line, &row); err != nil {
		return nil, err
	}
	return row, nil
}

// decodeSource 把 OffsetSource 的原始行解码为 Row；Stater/Positioner/
// Aligner 全部转发到内嵌的 OffsetSource（源是补偿侧）。
type decodeSource struct {
	src         *stream.OffsetSource
	unmarshaler RowUnmarshaler
}

func (s *decodeSource) Next(ctx context.Context, batchSize int) ([]datapipeline.Row, error) {
	lines, err := s.src.Next(ctx, batchSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	rows := make([]datapipeline.Row, 0, len(lines))
	for i, line := range lines {
		row, uerr := s.unmarshaler(line)
		if uerr != nil {
			return nil, fmt.Errorf("ingest: decode line %d: %w", i, uerr)
		}
		rows = append(rows, row)
	}
	return rows, err // 透传「最后一批 + io.EOF」组合
}

// Position 实现 stream.Positioner：转发源偏移。
func (s *decodeSource) Position() int64 { return s.src.Position() }

// MarshalState 实现 stream.Stater：转发源偏移状态。
func (s *decodeSource) MarshalState() (json.RawMessage, error) { return s.src.MarshalState() }

// UnmarshalState 实现 stream.Stater：转发源偏移还原。
func (s *decodeSource) UnmarshalState(data json.RawMessage) error { return s.src.UnmarshalState(data) }

// Align 实现 engine.Aligner：转发源侧补偿（校验源文件大小 ≥ LSN）。
func (s *decodeSource) Align(ctx context.Context, targetLSN int64) error {
	return s.src.Align(ctx, targetLSN)
}

// targetSink 把 DataTarget 适配为 stream.Sink（幂等重放由 DataTarget 契约
// 保证；目标不实现 Positioner——源才是补偿侧）。
type targetSink struct {
	target datapipeline.DataTarget
}

func (s *targetSink) Write(ctx context.Context, rows []datapipeline.Row) error {
	if _, err := s.target.WriteBatch(ctx, rows); err != nil {
		return fmt.Errorf("ingest: write batch: %w", err)
	}
	return nil
}
