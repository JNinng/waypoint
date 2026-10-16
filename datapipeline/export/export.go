// Package export 是导出垂直引擎：组合「分页 Source 适配（页游标经
// Stater 持久化，行编码在此完成）+ stream.ChunkedSink」。
//
// TaskType = "export"；Phases() = engine.DefaultPhaseSet()——合并收尾经
// Finalizer 在源耗尽时完成，无需声明中间业务阶段。
//
// 分块与最终文件名均以 outputPath 为前缀派生：
//
//	<outputPath>.chunk_<N>.tmp / <outputPath>.final.tmp / <outputPath>
//
// 调用顺序契约：engine.Run 返回 completed 后，调用方显式调用 Cleanup()
// 删除分块（ADR-3）。
package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/jninng/waypoint/datapipeline"
	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
	"github.com/jninng/waypoint/stream"
)

const taskType = "export"

// bindingNS 是本引擎在 Payload 中的自持命名空间（设计 §3：各参与方
// 自持子块、组合引擎负责拼装）。
const bindingNS = "export"

// RowMarshaler 把一行数据编码为原始行字节（不含行尾符）。
type RowMarshaler func(row datapipeline.Row) ([]byte, error)

// Engine 组装导出管道。零值不可用，经 New 构造。
type Engine struct {
	engine.Engine // stream.Pipeline.AsEngine("export")
	sink          *stream.ChunkedSink
	outputPath    string
}

// Option 配置导出引擎。
type Option func(*config)

type config struct {
	pageSize        int
	batchesPerChunk int
	rowMarshaler    RowMarshaler
}

// WithPageSize 设置分页大小（默认 1000）；n < 1 时 panic。
func WithPageSize(n int) Option {
	if n < 1 {
		panic(fmt.Sprintf("export: WithPageSize must be >= 1, got %d", n))
	}
	return func(c *config) { c.pageSize = n }
}

// WithBatchesPerChunk 转发到 stream.WithBatchesPerChunk（默认 10）。
func WithBatchesPerChunk(n int) Option {
	if n < 1 {
		panic(fmt.Sprintf("export: WithBatchesPerChunk must be >= 1, got %d", n))
	}
	return func(c *config) { c.batchesPerChunk = n }
}

// WithRowMarshaler 自定义行编码（默认 json.Marshal）。nil 时 panic。
func WithRowMarshaler(f RowMarshaler) Option {
	if f == nil {
		panic("export: WithRowMarshaler must not be nil")
	}
	return func(c *config) { c.rowMarshaler = f }
}

// New 创建导出引擎；outputPath 是最终输出文件路径（分块以其为前缀派生）。
func New(src datapipeline.DataSource, outputPath string, opts ...Option) *Engine {
	cfg := config{
		pageSize:     1000,
		rowMarshaler: defaultMarshaler,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	sink := stream.NewChunkedSink(filepath.Dir(outputPath), filepath.Base(outputPath))
	if cfg.batchesPerChunk > 0 {
		stream.WithBatchesPerChunk(cfg.batchesPerChunk)(sink)
	}
	p := stream.New[[]byte](&pageSource{ds: src, marshaler: cfg.rowMarshaler}, sink,
		stream.WithBatchSize[[]byte](cfg.pageSize))
	return &Engine{Engine: p.AsEngine(taskType), sink: sink, outputPath: outputPath}
}

// Cleanup 转发 ChunkedSink.Cleanup；终态 checkpoint 保存之后由调用方调用。
func (e *Engine) Cleanup() error { return e.sink.Cleanup() }

// Execute 重载内嵌引擎：先写入或校验绑定指纹，再委托管道。
func (e *Engine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if err := e.bindFingerprint(s); err != nil {
		return 0, err
	}
	return e.Engine.Execute(ctx, s)
}

type bindingState struct {
	Binding string `json:"binding"`
}

// bindingHash 计算绑定指纹（SHA-256 over 产物路径）。
func bindingHash(outputPath string) string {
	sum := sha256.Sum256([]byte(outputPath))
	return hex.EncodeToString(sum[:])
}

// bindFingerprint 维护自持命名空间的绑定指纹（ADR-10）：
//
//   - 新任务（PhasePending，含首个 checkpoint 的 pending→running 步）：
//     写入指纹——首个 checkpoint 必须携带，否则「crash 于首个 checkpoint
//     之后」的恢复将无指纹可校验；
//   - 恢复（其余阶段）：指纹缺失即拒绝（fail closed——留白等于绕过校验，
//     库无历史数据兼容负担），不一致拒绝（同 taskID 换产物路径）。
//
// 校验失败表现为可重试 error：WithRetry 会空转耗尽后返回，不落盘。
// 挂点说明：engine.Aligner 契约只传递 LSN（§5.2），指纹校验因此固定在
// Execute 首步；终态任务 Run 直接返回、不进 Execute，故不校验（终态无
// 副作用）。恢复同 taskID 必须绑定同源——源身份无法泛化哈希，为契约。
func (e *Engine) bindFingerprint(s *state.BaseTaskState) error {
	top, err := decodeNamespaces(s.Payload)
	if err != nil {
		return err
	}
	bind := bindingHash(e.outputPath)
	if raw, ok := top[bindingNS]; ok && len(raw) > 0 && string(raw) != "null" {
		var st bindingState
		if err := json.Unmarshal(raw, &st); err != nil {
			return fmt.Errorf("export: parse binding fingerprint: %w", err)
		}
		if st.Binding != bind {
			return fmt.Errorf("export: binding fingerprint mismatch for task %s: checkpoint bound to output %s, engine bound to %s — refusing to resume with a different output path", s.TaskID, st.Binding, bind)
		}
		return nil
	}
	if s.Phase != state.PhasePending {
		return fmt.Errorf("export: checkpoint for task %s lacks binding fingerprint: refusing to resume", s.TaskID)
	}
	raw, err := json.Marshal(bindingState{Binding: bind})
	if err != nil {
		return fmt.Errorf("export: marshal binding fingerprint: %w", err)
	}
	top[bindingNS] = raw
	out, err := json.Marshal(top)
	if err != nil {
		return fmt.Errorf("export: marshal payload: %w", err)
	}
	s.Payload = out
	return nil
}

// decodeNamespaces 把任务 Payload 解析为顶层命名空间表（空/null → 空表）。
// 与 ingest 各自持有一份——垂直包独立演进（ADR-8），命名空间自持（§3）。
func decodeNamespaces(payload json.RawMessage) (map[string]json.RawMessage, error) {
	top := map[string]json.RawMessage{}
	if len(payload) == 0 || string(payload) == "null" {
		return top, nil
	}
	if err := json.Unmarshal(payload, &top); err != nil {
		return nil, fmt.Errorf("export: parse task payload: %w", err)
	}
	if top == nil {
		top = map[string]json.RawMessage{}
	}
	return top, nil
}

func defaultMarshaler(row datapipeline.Row) ([]byte, error) {
	return json.Marshal(row)
}

// pageSource 把 DataSource 适配为 stream.Source[[]byte]：
// 页游标经 Stater 持久化，行编码在此完成。
type pageSource struct {
	ds        datapipeline.DataSource
	marshaler RowMarshaler
	nextPage  int
}

// Next 拉取下一页并编码；空页（0 行且无错误）视为耗尽。
func (s *pageSource) Next(ctx context.Context, batchSize int) ([][]byte, error) {
	rows, err := s.ds.FetchPage(ctx, s.nextPage, batchSize)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("export: fetch page %d: %w", s.nextPage, err)
	}
	if len(rows) == 0 {
		return nil, io.EOF
	}
	s.nextPage++
	items := make([][]byte, 0, len(rows))
	for i, row := range rows {
		b, merr := s.marshaler(row)
		if merr != nil {
			return nil, fmt.Errorf("export: marshal row %d of page %d: %w", i, s.nextPage-1, merr)
		}
		items = append(items, b)
	}
	if errors.Is(err, io.EOF) {
		return items, io.EOF
	}
	return items, nil
}

// MarshalState 实现 stream.Stater：页游标（下一页索引）。
func (s *pageSource) MarshalState() (json.RawMessage, error) {
	raw, err := json.Marshal(struct {
		NextPage int `json:"next_page"`
	}{NextPage: s.nextPage})
	if err != nil {
		return nil, fmt.Errorf("export: marshal page cursor: %w", err)
	}
	return raw, nil
}

// UnmarshalState 实现 stream.Stater：全量还原（nil = 回退初始）。
func (s *pageSource) UnmarshalState(data json.RawMessage) error {
	if len(data) == 0 || string(data) == "null" {
		s.nextPage = 0
		return nil
	}
	var st struct {
		NextPage int `json:"next_page"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("export: parse page cursor: %w", err)
	}
	if st.NextPage < 0 {
		return fmt.Errorf("export: invalid page cursor %d", st.NextPage)
	}
	s.nextPage = st.NextPage
	return nil
}
