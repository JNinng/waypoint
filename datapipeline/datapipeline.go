// Package datapipeline 是数据管道垂直扩展包（独立分发，核心库不依赖它，
// 见 docs/design.md §5.5 与 ADR-8）。
//
// 它定义导出/导入两个垂直引擎共用的词汇：Row 数据行、DataSource 分页
// 数据源与 DataTarget 幂等写入目标。具体引擎见子包 export 与 ingest。
package datapipeline

import "context"

// Row 是一行业务数据（DB 记录 / API 对象），值为 JSON 可序列化类型。
type Row = map[string]any

// DataSource 是分页数据源（DB 查询 / API）。page 从 0 起；io.EOF 表示耗尽。
//
// 契约：同一 page 的重复拉取应返回相同数据（页可重放）。页不可幂等重放的
// 数据源须自行在适配层转为可重放游标（设计 §5.5 失败模式）。
type DataSource interface {
	FetchPage(ctx context.Context, page, pageSize int) ([]Row, error)
}

// DataTarget 是幂等写入目标（UPSERT / 去重）——重放同一批安全。
type DataTarget interface {
	WriteBatch(ctx context.Context, rows []Row) (inserted int64, err error)
}
