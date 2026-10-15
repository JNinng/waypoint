// Package state 定义任务状态的领域模型与原子持久化契约。
//
// BaseTaskState 是所有任务的公共状态骨架，业务状态经 Payload 扩展；
// StateRepository 是状态持久化的唯一抽象（Load 缺失返回 (nil, nil)、
// Save 原子全量替换、Delete 幂等）。持久化语义分级（进程崩溃安全默认保证，
// 掉电安全可选）由后端实现，见 filestore 与 internal/atomicfile。
//
// 并发边界：StateRepository 实现须支持多任务并发访问（按 taskID 隔离）；
// 单任务单执行器前提由调用方保证（同一 taskID 同时至多一个 Run 推进）。
package state

import (
	"context"
	"encoding/json"
	"errors"
)

// Phase 是异步任务的生命周期阶段。
// 字符串值保持稳定——它们会被持久化到 checkpoint 中。
type Phase string

// 协议级常量，构成默认生命周期：pending → running → completed / failed。
// 业务阶段由引擎在 PhaseSet 中声明，不作为核心包常量。
const (
	PhasePending   Phase = "pending"
	PhaseRunning   Phase = "running"
	PhaseCompleted Phase = "completed"
	PhaseFailed    Phase = "failed"
)

// LSN 是不透明的进度偏移量（int64 覆盖文件字节、行号、DB WAL LSN、
// 时间戳等场景）。框架只透传、不解释；对齐语义由引擎/适配层定义。
type LSN = int64

// BaseTaskState 是公共状态字段，业务状态经 Payload 扩展。
//
// Payload 顶层是命名字段空间（如 {"pipeline": {...}, "outbox": [...]}），
// 各参与方自持子块、可组合共存；子块的合并与拆分由组合引擎负责，
// 框架不提供合并助手。
type BaseTaskState struct {
	TaskID        string          `json:"task_id"`
	TaskType      string          `json:"task_type"`
	Phase         Phase           `json:"phase"`
	Message       string          `json:"message,omitempty"` // 失败原因 / 进度描述
	Progress      int             `json:"progress"`          // 0-100
	CheckpointLSN LSN             `json:"checkpoint_lsn"`
	Payload       json.RawMessage `json:"payload"`
}

// StateRepository 是任务状态的原子持久化抽象。
//
// 契约：
//   - Load：任务不存在返回 (nil, nil)，不是 error。
//   - Save：原子全量替换；返回 nil 表示已完整持久化，不允许部分合并。
//   - Delete：幂等；删除不存在的任务不报错，IO 故障必须返回错误。
//   - 一切方法接收 context 并响应取消。
type StateRepository interface {
	Load(ctx context.Context, taskID string) ([]byte, error)
	Save(ctx context.Context, taskID string, state []byte) error
	Delete(ctx context.Context, taskID string) error
}

// 持久化类别哨兵。后端保存/加载失败时须与底层根因一并挂载
// （errors.Join 或等价多 %w），使 errors.Is 对二者均可达。
var (
	ErrSaveFailed = errors.New("waypoint/state: save failed")
	ErrLoadFailed = errors.New("waypoint/state: load failed")
)
