// Package waypoint 是便捷调用糖的组合边界：把「状态目录 + 引擎 + taskID」
// 绑定为一次任务世系（Task），并可经 RunWithOutbox 桥接 outbox 调度层。
//
// 定位（ADR-10）：糖只承载工学——把正确用法缩短为一次构造 + 一次 Run。
// 安全主张由各机制的自身校验承载：引擎维度是恢复时的 TaskType 校验，
// 绑定维度是垂直引擎的绑定指纹（缺失即拒、错配即拒）。Task 不拦截
// 「同 id 不同绑定」的构造——那在进程内无法判定意图，跨进程场景由
// 指纹拒绝。
//
// 非目标：Task 不提供 Delete（无消费方）；本包不 re-export state 常量；
// 不为 saga 提供糖（其调用已足够简短，糖的准入门槛是繁琐度）。
package waypoint

import (
	"context"
	"fmt"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/state"
)

// Task 是一次任务世系的句柄：状态目录、引擎（含源/产物绑定）与 taskID
// 的不可变绑定。三者同生共死——跨进程恢复时以相同三元组重建 Task；
// 产物/源路径的错配由垂直引擎的绑定指纹在恢复时拒绝。
//
// dir 合法性在 Run 时校验（构造不触盘）。
type Task struct {
	dir    string
	eng    engine.Engine
	taskID string
}

// NewTask 绑定一次任务世系。eng 内的源/产物绑定必须与 taskID 匹配：
// 恢复同 taskID 时必须绑定同源同产物（产物/源路径由绑定指纹强制，
// 源内容身份为契约，见各垂直引擎文档）。
func NewTask(dir string, eng engine.Engine, taskID string) *Task {
	return &Task{dir: dir, eng: eng, taskID: taskID}
}

// Run 运行/恢复该世系到终态，等价 filestore.New(dir) + engine.Run(...)。
// dir 不可创建/不可读等构造错误在此时返回。
func (t *Task) Run(ctx context.Context, opts ...engine.RunOption) (*state.BaseTaskState, error) {
	repo, err := filestore.New(t.dir)
	if err != nil {
		return nil, fmt.Errorf("waypoint: %w", err)
	}
	return engine.Run(ctx, repo, t.eng, t.taskID, opts...)
}

// RunWithOutbox 运行世系并把最终状态的 outbox 命名空间对齐到 Store
// （等价 outbox.RunWithOutbox，重复 ID 幂等跳过）；分发仍由调用方的
// Dispatcher 负责。
func (t *Task) RunWithOutbox(ctx context.Context, store outbox.Store, opts ...engine.RunOption) (*state.BaseTaskState, error) {
	repo, err := filestore.New(t.dir)
	if err != nil {
		return nil, fmt.Errorf("waypoint: %w", err)
	}
	return outbox.RunWithOutbox(ctx, repo, t.eng, t.taskID, store, opts...)
}
