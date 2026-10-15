# waypoint

为长时间运行的异步任务提供 **checkpoint / resume** 能力的纯 Go 库（Go 1.26+，零外部依赖，仅标准库）。调用方只需描述「一步做什么」，框架负责初始化、原子持久化、崩溃恢复编排与进程内重试。

适用场景：ETL 流水线、数据同步、批处理等一切「可分步推进且需中断恢复」的任务。

> 设计规格、架构决策与验收清单见 [docs/](docs/)。本文只描述代码当前提供的能力。

## 包结构

| 包 | 职责 |
|---|---|
| `state` | 领域模型：`Phase` / `LSN` / `BaseTaskState` / `StateRepository` 与持久化哨兵错误 |
| `engine` | 通用编排主循环 `Load → 对齐 → Execute → Save`；`PhaseSet` 阶段词汇表；`Run` / `WithRetry`（快照重试） |
| `stream` | 批次管道原语 `Source` / `Sink` / `Pipeline`（含 `AsEngine` 适配）；库内文件组件 `ChunkedSink`（分块文件汇）与 `OffsetSource`（偏移行源） |
| `filestore` | `StateRepository` 的文件后端（每任务一文件，tmp→Sync→Rename 原子写） |
| `outbox` | 调度层 outbox 模式：`Accumulator`（意图收集）→ `Store`（持久化）→ `Dispatcher`（at-least-once 分发）；`RunWithOutbox` 桥接引擎；`outbox/filestore` 文件后端 |
| `saga` | 调度层 saga 模式：多步补偿事务（终态三分、步骤指纹校验、补偿可重试/超时） |
| `datapipeline`（独立模块） | 导出/导入垂直引擎：`export`（分页源 + ChunkedSink）、`ingest`（OffsetSource + 幂等目标） |

依赖方向：`datapipeline` → 核心包；`outbox`/`saga` 是独立状态机（`outbox.RunWithOutbox` 桥接 `engine`）；一切持久化组件复用 `internal/atomicfile` 原子写原语。

## 快速上手

### 分页导出（断点续传）

```go
repo, _ := filestore.New("states")                    // 状态目录
eng := export.New(dbSource, "out/orders.jsonl",       // 分页数据源 + 输出路径
    export.WithPageSize(1000),
    export.WithBatchesPerChunk(10))

final, err := engine.Run(ctx, repo, eng, "export-42") // 崩溃后重跑即续传
if err != nil { /* 可重试/可恢复：不落盘，下次 Run 从最后 checkpoint 恢复 */ }
if final.Phase == state.PhaseCompleted {
    eng.Cleanup()                                     // 终态落盘后清理分块
}
```

### JSONL 导入（偏移恢复）

```go
eng := ingest.New("in/events.jsonl", upsertTarget, ingest.WithBatchSize(1000))
final, err := engine.Run(ctx, repo, eng, "ingest-42")
// 补偿侧是源：恢复时校验源文件大小 ≥ checkpoint LSN，源被截断则拒绝恢复。
```

### 自定义引擎

```go
type myEngine struct{ /* ... */ }

func (e *myEngine) TaskType() string                     { return "sync" }
func (e *myEngine) Phases() engine.PhaseSet              { return engine.DefaultPhaseSet() }
func (e *myEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
    // 执行一步业务并返回新 LSN；副作用必须幂等或可对齐。
    // 优雅失败：置 s.Phase = state.PhaseFailed 并返回 nil（唯一落盘失败路径）。
}
```

## 示例

各场景的可运行示例见 [examples/](examples/)（独立模块，在本目录下 `go run ./<场景>`）：
`custom-engine`（最小引擎与恢复）、`export`（分页导出断点续传）、`ingest`（JSONL 导入）、`outbox`（意图落盘与分发）、`saga`（补偿事务与 Resume），与设计 §6 关键流程一一对应。

## 关键语义（速查）

- **失败语义**：`Execute` 返回 error = 可重试/可恢复（不落盘，下次 Run 恢复）；引擎在 `Execute` 内置 `PhaseFailed` 返回 nil = 优雅失败（唯一落盘失败路径）。框架不兜底写终态。
- **LSN**：不透明 `int64`，框架只透传；管道按补偿侧配对规则计算（`Sink.Position()` → `Source.Position()` → 累计项数）。
- **并发模型**：库不创建 goroutine（唯一例外 `Dispatcher.Start` 轮询便利模式）；单任务单执行器、单 saga 单协调器、单分发器（详见 `docs/design.md` §2.4）。
- **不可逆操作**（发邮件、扣款、消息队列发送）不得在引擎 `Execute` 内直接执行——经 outbox / saga 在调度层处理。
- **非目标**：任务调度器、定时器、并发池、HTTP/gRPC 服务层、多实例互斥、掉电安全默认保证（父目录 fsync 为可选启用项，未验证前不提供入口）。

## 构建 与 测试

仓库含两个模块：核心模块（根）与 `datapipeline` 扩展模块（嵌套模块，
`./...` 模式会跳过它，需分别运行）：

```bash
go build ./... && go test ./...     # 核心模块
cd datapipeline && go test ./...    # 扩展模块
```
