# waypoint 使用指南

各组件的用法、契约与调用方义务。
设计动机与决策记录见 [design.md](design.md)（设计规格与术语表）、[adr.md](adr.md)；能力速览见 [README](../README.md)。

## 目录

- [1. waypoint 是什么](#1-waypoint-是什么)
- [2. 引入](#2-引入)
- [3. 核心概念速览](#3-核心概念速览)
- [4. 编写与运行引擎](#4-编写与运行引擎)
  - [4.1 最小引擎](#41-最小引擎) · [4.2 失败语义](#42-失败语义) · [4.3 进程内重试](#43-进程内重试) · [4.4 运行：显式路径与便捷糖](#44-运行显式路径与便捷糖) · [4.5 恢复与绑定指纹](#45-恢复与绑定指纹)
- [5. 批次管道（stream）](#5-批次管道stream)
  - [5.3 ChunkedSink（分块文件汇）](#53-库内组件chunkedsink分块文件汇) · [5.4 OffsetSource（偏移行源）](#54-库内组件offsetsource偏移行源)
- [6. 数据管道扩展包（datapipeline）](#6-数据管道扩展包datapipeline)
  - [6.1 export：分页导出断点续传](#61-export分页导出断点续传) · [6.2 ingest：JSONL 导入偏移恢复](#62-ingestjsonl-导入偏移恢复)
- [7. 状态持久化（state / filestore）](#7-状态持久化state--filestore)
- [8. Outbox：不可逆操作的意图先落盘](#8-outbox不可逆操作的意图先落盘)
  - [8.1 引擎内收集意图](#81-引擎内收集意图) · [8.2 桥接：意图对齐到 Store](#82-桥接意图对齐到-store) · [8.3 分发](#83-分发) · [8.4 Store 契约](#84-store-契约)
- [9. Saga：多步补偿事务](#9-saga多步补偿事务)
- [10. 错误判别](#10-错误判别)
- [11. 并发模型与调用方义务](#11-并发模型与调用方义务)
- [12. 默认值速查](#12-默认值速查)
- [13. 示例索引](#13-示例索引)

按场景直达：自定义任务引擎 → [§4](#4-编写与运行引擎)；崩溃恢复 / 断点续传 → [§4.5](#45-恢复与绑定指纹)；分页导出 → [§6.1](#61-export分页导出断点续传)；JSONL 导入 → [§6.2](#62-ingestjsonl-导入偏移恢复)；不可逆操作分发 → [§8](#8-outbox不可逆操作的意图先落盘)；多步补偿事务 → [§9](#9-saga多步补偿事务)；换状态后端 → [§7](#7-状态持久化state--filestore)

---

## 1. waypoint 是什么

waypoint 是为长时间运行的异步任务提供 **checkpoint / resume** 能力的纯 Go 库（Go 1.26+，零外部依赖，仅标准库）。调用方只需描述「一步做什么」（引擎契约），框架负责初始化、原子持久化、崩溃恢复编排与进程内重试。

适用场景：ETL 流水线、数据同步、批处理等一切「可分步推进且需中断恢复」的任务。

**不做什么**（非目标）：任务调度器、定时器、并发 goroutine 池、HTTP/gRPC 服务层、多实例/多进程互斥、掉电安全默认保证。

分层一览：

| 层 | 包 | 作用 |
|---|---|---|
| 组合糖 | `waypoint` | 把「状态目录 × 引擎 × taskID」绑定为一次可恢复任务（`Task`） |
| 引擎层 | `engine` | 通用主循环 Load → 对齐 → Execute → Save |
| 管道层 | `stream` | 批次管道原语 + 两个库内文件组件（`ChunkedSink` / `OffsetSource`） |
| 垂直引擎 | `datapipeline`（独立模块） | 分页导出 `export` / JSONL 导入 `ingest`，开箱即用 |
| 持久化 | `state` / `filestore` | 状态契约与文件后端 |
| 调度层 | `outbox` / `saga` | 不可逆操作的 at-least-once 分发 / 多步补偿事务 |

---

## 2. 引入

核心库与扩展包是两个 Go module，按需引入：

```go
import (
    "github.com/jninng/waypoint/engine"
    "github.com/jninng/waypoint/state"
    "github.com/jninng/waypoint/waypoint"

    // 扩展包（独立 module，单独 require）
    "github.com/jninng/waypoint/datapipeline/export"
    "github.com/jninng/waypoint/datapipeline/ingest"
)
```

仓库自带可运行示例（独立 module）：`cd examples && go run ./<场景>`，见文末[示例索引](#13-示例索引)。

---

## 3. 核心概念速览

| 概念 | 含义 |
|---|---|
| checkpoint | 一次成功 Execute 后原子持久化的任务状态（含 LSN 与 Payload） |
| 引擎（Engine） | 任务契约：声明阶段词汇表 + 实现「执行一步业务」 |
| Phase / PhaseSet | 生命周期阶段 / 引擎声明的阶段集合；默认 `pending → running → completed / failed` |
| LSN | 不透明进度偏移量（`int64`）；框架只透传，语义由引擎定义 |
| 世系（lineage） | 一次可恢复任务的身份三元组 `(状态目录, 引擎, taskID)`，同生共死 |
| 绑定指纹 | 垂直引擎写入 checkpoint 的路径哈希；恢复时缺失即拒、错配即拒 |
| 补偿侧 | 持有物理位置、恢复时需要对齐的一侧（导出=汇，导入=源） |
| 优雅失败 | 引擎在 Execute 内置 `PhaseFailed` 并返回 nil 的失败路径——**唯一落盘失败路径** |

三条失败/结束路径，先记住这条主线（ADR-1）：

| Execute 的返回 | 框架行为 | 是否落盘 |
|---|---|---|
| `nil` 且 Phase 已达终态 | 保存终态 checkpoint，Run 返回 | 是 |
| `nil` 且 Phase = `PhaseFailed` | 保存 failed checkpoint，Run 返回（**优雅失败**） | 是 |
| 非 nil error | 重试（若配置）；耗尽后 Run 返回该 error | **否**，任务停留于最后 checkpoint |

框架**从不**兜底写终态；ctx 取消同样不落盘。「未完成」与「失败告终」因此天然可区分：持久化的 `failed` 只可能来自引擎显式判定。

---

## 4. 编写与运行引擎

### 4.1 最小引擎

```go
type myEngine struct{ /* 业务依赖 */ }

func (e *myEngine) TaskType() string        { return "sync" }
func (e *myEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }

func (e *myEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
    if s.Phase == state.PhasePending { // 首步：进入 running（本步也会保存首个 checkpoint）
        s.Phase = state.PhaseRunning
        return 0, nil
    }
    // 从 checkpoint 推导本步起点，执行一步业务，返回新 LSN。
    // 时序契约：先产生物理副作用，框架后保存 checkpoint。
    // 重入契约：副作用必须幂等，或可由恢复路径的 Align 对齐。
    done, err := e.step(ctx, s)
    if err != nil {
        return 0, err // 可重试/可恢复：不落盘
    }
    if done {
        s.Phase = state.PhaseCompleted
    }
    return s.CheckpointLSN + 1, nil // LSN 语义由你定义，并在引擎文档声明
}
```

引擎契约三方法：

- `TaskType() string`——任务类型标识。框架恢复时校验 checkpoint 的 TaskType 与引擎一致，防止用错引擎恢复旧状态。
- `Phases() engine.PhaseSet`——阶段词汇表。无业务阶段时用 `engine.DefaultPhaseSet()`；自定义时须满足校验（`All` 非空、`Initial ∈ All` 且非终态、`Recoverable`/`Terminal ⊆ All`、`PhaseFailed ∈ All` 则必须在 `Terminal`）。
- `Execute(ctx, *state.BaseTaskState) (state.LSN, error)`——执行一步。直接修改 `s`（框架在每次 Execute 前深快照，失败调用回滚后重试，部分修改不会泄漏）。

可选能力用接口探测，**不实现即不具备，不留空方法**：

| 接口 | 方法 | 用途 |
|---|---|---|
| `engine.Aligner` | `Align(ctx, targetLSN) error` | 恢复时把物理系统对齐到 checkpoint（如文件截断、偏移校验）；不得依赖 Stater 内容——需要的元数据须可逆编码进 LSN |
| `engine.Progressor` | `Progress(s) int` | 写入 `s.Progress`（0-100）；无依据时可为 0，不得伪造 |

### 4.2 失败语义

- **优雅失败**（任务确实失败、无需再跑）：`s.Phase = state.PhaseFailed`，失败原因写 `s.Message`，返回 `nil`。框架保存后停止，这是唯一落盘的失败路径。
- **可重试错误**（瞬态故障）：返回 error。任务停留于最后 checkpoint，故障消除后**重跑 Run 即恢复**；长期不再跑的任务在存储上呈现为非终态（等价于崩溃中断），可经 `Message`/外部日志辅助排查。

### 4.3 进程内重试

```go
final, err := engine.Run(ctx, repo, eng, "task-1",
    engine.WithRetry(3, time.Second)) // 失败回滚到本步快照后重试，最多额外 3 次，间隔 1s
```

- 重试以**快照为起点**（ADR-5）：失败调用的状态修改自动回滚，你无需防御性恢复。
- 只作用于 Execute；Load/Save/Align 错误不重试，直接返回。
- 默认不重试（`maxRetries=0`）；负值参数构造时 panic。

### 4.4 运行：显式路径与便捷糖

两条路径等价，任选其一：

```go
// 显式路径：自持 StateRepository
repo, err := filestore.New("states")
final, err := engine.Run(ctx, repo, eng, "task-1", engine.WithRetry(3, 0))

// 便捷糖：Task 世系句柄绑定（状态目录 × 引擎 × taskID）（ADR-10）
task := waypoint.NewTask("states", eng, "task-1")
final, err := task.Run(ctx, engine.WithRetry(3, 0))
```

- `Run` 成功返回**最终已保存状态**（终态），失败返回 `(nil, err)`。
- 终态任务的重复 Run 直接返回已存状态，无任何副作用（也不做指纹校验）。
- ctx 取消：返回 `ctx.Err()`，不落盘，下次 Run 恢复。
- `Task` 构造不触盘；目录错误延迟到 Run 时返回。
- 糖只缩短正确用法，不拦截错误用法——安全由 TaskType 校验与绑定指纹承载（下节）。

### 4.5 恢复与绑定指纹

断点续传的主场景是**跨进程重建**：以相同三元组（状态目录、同源同产物的引擎、同 taskID）重新构造 Task/引擎，再 Run 即从最后 checkpoint 恢复。引擎实例不需要、也不应该携带进度记忆——checkpoint 是唯一恢复依据；同一引擎实例在同任务内跨 Run 复用同样安全（管道组件每次 Execute 全量还原）。

恢复安全由两层机制强制：

1. **TaskType 校验**（框架）：checkpoint 与引擎的 TaskType 不一致 → 拒绝恢复。
2. **绑定指纹**（`export`/`ingest` 引擎，ADR-10）：垂直引擎把产物/源路径哈希写入 checkpoint 自持命名空间——首个 checkpoint 即携带；恢复时**缺失即拒**（fail closed）、**不一致即拒**（同 taskID 换路径），错误为可重试 error。

调用方契约：**一产物路径一世系**（多世系共享输出路径会互踩分块）；源文件内容（同路径换数据）无法机制化校验，属调用方义务，导入场景由源大小校验部分兜底（§7.2）。

---

## 5. 批次管道（stream）

自建管道只需一对薄适配器。循环骨架「取一批 → 写一批 → 落 checkpoint」由 `Pipeline` 承担：

```go
type lines struct{ /* 你的源 */ }
func (s *lines) Next(ctx context.Context, batchSize int) ([][]byte, error) { /* ... */ }

type db struct{ /* 你的幂等汇 */ }
func (d *db) Write(ctx context.Context, items [][]byte) error { /* UPSERT 等 */ }

eng := stream.New[[]byte](&lines{}, &db{}, stream.WithBatchSize[[]byte](500)).AsEngine("my-pipe")
final, err := waypoint.NewTask("states", eng, "pipe-1").Run(ctx)
```

### 5.1 Source / Sink 契约

- `Source.Next(ctx, batchSize)`：返回下一批；**最后一批允许与 `io.EOF` 同次返回**（先处理本批再终止）；彻底耗尽返回 `(nil, io.EOF)`。
- `Sink.Write(ctx, items)`：**必须幂等**——崩溃恢复会重放同一批。

### 5.2 可选接口与 LSN 配对规则

| 接口 | 语义 |
|---|---|
| `Stater` | 源/汇自身的持久化状态（游标、偏移等），随 checkpoint 原子保存。`UnmarshalState` 必须**全量还原**到上次已提交状态（`nil` 入参 = 回退初始）——增量还原会导致重放重复 |
| `Positioner` | 报告已提交物理边界（`Write` 之后调用）。**只有补偿侧可实现，且必须同时实现 `engine.Aligner`**；Sink 与 Source 不得同时实现。违反配对规则在 Execute/Align 时报错 |
| `Sizer` | 总量估计，配合 Positioner 估算进度 |
| `Finalizer` | 源耗尽后的收尾（如分块原子合并）；不得删除重放输入 |

每步成功 Write 后的 LSN 按序回退：

```
LSN = Sink.Position()  →  否则 Source.Position()  →  否则 累计已处理项数
```

### 5.3 库内组件：ChunkedSink（分块文件汇）

把批次逐行写入分块临时文件，收尾时原子合并为最终文件。实现 `Sink[[]byte]`（每项一行）+ `Stater` + `Finalizer`；项为**不含行尾符**的原始行字节，行编码由你的 Source 适配层完成。

```go
sink := stream.NewChunkedSink(dir, "orders.jsonl",
    stream.WithBatchesPerChunk(100), // 默认 10 批一块
    stream.WithLineSuffix([]byte("\n")), // 默认 '\n'；传 nil 表示不追加
    stream.WithFilePerm(0o644))
```

文件布局（均相对 `dir` 派生自 `finalName`，可含子目录前缀）：

```
orders.jsonl.chunk_<N>.tmp   分块（重放输入，Finalize 后仍保留）
orders.jsonl.final.tmp       合并临时文件（成功后原子 Rename）
orders.jsonl                 最终产物
```

崩溃安全机制是**截断重放**：恢复重放时先截断当前分块到 checkpoint 记录的已提交字节边界，再追加本批——已写未存的多余字节被消除，重放不重复、不乱序。分块模式下无 Positioner/Aligner，管道 LSN 回退「累计已处理项数」。

**Cleanup 协议（ADR-3）**：分块是崩溃恢复的重放输入，删除必须推迟到 completed checkpoint 落盘**之后**、由调用方显式执行，且错误不得静默：

```go
if final.Phase == state.PhaseCompleted {
    if err := sink.Cleanup(); err != nil {
        return err // 目录读取/删除失败可见
    }
}
```

### 5.4 库内组件：OffsetSource（偏移行源）

按字节偏移逐行读取文本文件（JSONL 等），断点续读。实现 `Source[[]byte]`（返回不含行尾符的原始行）+ `Stater` + `Positioner` + `engine.Aligner`——**源即补偿侧**，恢复时 `Align` 校验源文件大小 ≥ checkpoint LSN，小于则拒绝（源可能被截断/替换）。

```go
src := stream.NewOffsetSource("in/events.jsonl")
```

字节偏移按**含行分隔符的原始字节数**累计；兼容 `\n`、`\r\n`、末行无换行，不支持裸 `\r` 行尾。

---

## 6. 数据管道扩展包（datapipeline）

独立 module 的两个参考级垂直引擎，组合 §5 的组件开箱即用；任一侧适配器（数据源/目标/编解码）都可替换为自定义实现。两者都带绑定指纹（§4.5）。

### 6.1 export：分页导出断点续传

```go
eng := export.New(dbSource, "out/orders.jsonl", // DataSource + 输出路径
    export.WithPageSize(1000),                  // 默认 1000
    export.WithBatchesPerChunk(50),             // 转发 ChunkedSink，默认 10
    export.WithRowMarshaler(func(r datapipeline.Row) ([]byte, error) {
        return json.Marshal(r) // 默认 json.Marshal；可换 CSV 等
    }))

task := waypoint.NewTask("states", eng, "export-42")
final, err := task.Run(ctx)
if err != nil { /* 可重试/可恢复：不落盘，重跑即续传 */ }
if final.Phase == state.PhaseCompleted {
    if err := eng.Cleanup(); err != nil { /* 终态落盘后清理分块（ADR-3） */ }
}
```

- `DataSource` 契约：`FetchPage(ctx, page, pageSize)`，`page` 从 0 起；`io.EOF` 表示耗尽（可与最后一批同次返回），空页（0 行且无错误）同样视为耗尽。同一 page 的重复拉取应返回相同数据（页可重放）；页不可重放的数据源须自行转为可重放游标。
- TaskType = `"export"`；分页游标经 Stater 随 checkpoint 原子保存；LSN = 累计已处理行数。
- 分块与最终文件名均派生自 `outputPath`（§5.3 布局）；换 `outputPath` 重跑同 taskID 会被绑定指纹拒绝。

### 6.2 ingest：JSONL 导入偏移恢复

```go
eng := ingest.New("in/events.jsonl", upsertTarget, // 源文件路径 + 幂等目标
    ingest.WithBatchSize(1000),                    // 默认 1000
    ingest.WithRowUnmarshaler(func(line []byte) (datapipeline.Row, error) {
        var r datapipeline.Row
        return r, json.Unmarshal(line, &r) // 默认 json.Unmarshal
    }))

final, err := waypoint.NewTask("states", eng, "ingest-42").Run(ctx)
```

- `DataTarget` 契约：`WriteBatch(ctx, rows)` **必须幂等**（UPSERT/去重）——「写成功但确认丢失」的崩溃会重放同一批，重放安全由目标消化。
- TaskType = `"ingest"`；补偿侧是**源**：恢复时先校验源文件大小 ≥ checkpoint LSN（源被截断则拒绝），再从字节偏移续读。LSN = 源文件字节偏移。
- 换源路径重跑同 taskID 会被绑定指纹拒绝。

---

## 7. 状态持久化（state / filestore）

### 7.1 契约

`state.StateRepository` 是状态持久化的唯一抽象：

| 方法 | 契约 |
|---|---|
| `Load` | 任务不存在返回 `(nil, nil)`，**不是 error** |
| `Save` | 原子全量替换；返回 nil 表示已完整持久化，不允许部分合并 |
| `Delete` | 幂等：目标不存在视为成功，IO 故障必须返回错误 |

一切方法接收 ctx 并响应取消。实现方须自行保证进程崩溃安全（「半写的正式文件」任何时刻不存在）。

### 7.2 文件后端

```go
repo, err := filestore.New("states") // MkdirAll 0755，并清理崩溃残留 *.state.tmp
```

- 每任务一文件：`<taskID>.state`；进程崩溃安全（tmp → Sync → Rename）默认保证。
- **掉电安全**（父目录 fsync）默认关闭，当前未提供启用入口（Windows 行为验证中，见 checklists #1）。
- `taskID` 须为文件系统安全字符串：非空、非 `.`/`..`、不含路径分隔符与 Windows 保留字符 `\ / : * ? " < > |`、不含控制字符；非法时读写均报错。
- outbox 消息后端 `outbox/filestore` 同构：`<msgID>.msg.json`，支持 Append 与 FetchPending 并发。

---

## 8. Outbox：不可逆操作的意图先落盘

发邮件、扣款、消息队列发送等**不可逆操作不得在引擎 Execute 内直接执行**——先经意图收集随 checkpoint 原子落盘（首选 `outbox/producer` 骨架，或手写 `Collector`），再由调度层以 at-least-once 分发。

### 8.1 引擎内收集意图

首选 `outbox/producer` 骨架：业务方只提供**记录源**（副作用在 `Next` 内幂等完成）与**意图派生函数**，引导步、已落盘意图还原、分批推进、合并落盘、耗尽收尾由组件固定（ADR-12）：

```go
eng := producer.New("arrival-notify", &orderSource{ /* ... */ },
    func(ctx context.Context, taskID string, o order) (outbox.Message, error) {
        return outbox.Message{
            ID:        "notify-" + o.ID, // 业务键派生：稳定、重放可识别
            TaskID:    taskID,
            EventType: "arrival-notify",
            Payload:   payloadOf(o),
        }, nil
    },
    producer.WithBatchSize[order](200))

final, err := waypoint.NewTask("states", eng, "notify-1").Run(ctx)
// LSN = 已提交记录数；源耗尽自动 completed；意图随 checkpoint 原子落盘。
// 派生失败为可重试 error：重放时已收集意图由组件幂等消化、副作用由
// Source 的幂等契约消化。
```

`Source.Next(ctx, after, batchSize)` 的 `after` 是已提交记录数，切片/分页型源据此定位本批；副作用必须幂等（重放同一批不累积）。不可由记录数定位的游标型源暂不支持（checklists 待确认 #5）。

需要混合业务与意图收集（引擎还有其他工作）时，用 `Collector` 手写：

```go
type myEngine struct{ col outbox.Collector }

func (e *myEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
    // 步首：还原已落盘意图进收集器（每实例一次生效，推荐每步无条件调用）。
    // 跳过它直接 Flush，会以「只有新意图」的视图覆盖命名空间、丢掉已落盘意图。
    if err := e.col.Restore(s); err != nil {
        return 0, err
    }
    // ... 业务副作用 + 意图派生收集 ...
    // 步末：收集器全量视图合并进 Payload 的 outbox 命名空间（保留其他命名空间），
    // 随本步 checkpoint 原子落盘。
    if err := e.col.Flush(s); err != nil {
        return 0, err
    }
    // ...
}
```

`Accumulator` 是严格收集原语（重复 ID 返回 `ErrDuplicateID`，供需要显式判别的场景）；`Collector` 在其上固化「还原 → 收集 → 合并」装配并对重放收集幂等（ADR-11），常规生产引擎用它。

### 8.2 桥接：意图对齐到 Store

```go
final, err := waypoint.NewTask("states", eng, "task-1").RunWithOutbox(ctx, store)
// 等价 outbox.RunWithOutbox(ctx, repo, eng, taskID, store)
```

Run 成功后，最终状态的 `outbox` 命名空间与 Store 逐条对齐：缺失则幂等 Append（`ErrDuplicateID` 跳过）。这覆盖「Run 成功但 Append 前崩溃」窗口——重调 `RunWithOutbox` 幂等恢复。对齐后的分发由你自己的 Dispatcher 完成。

### 8.3 分发

```go
registry := outbox.NewHandlerRegistry(map[string]outbox.Handler{
    "email": func(ctx context.Context, m outbox.Message) error {
        return sendEmail(ctx, m) // 必须幂等：同一条消息可能被投递多次；须响应 ctx 取消
    },
})
d := outbox.NewDispatcher(store, registry,
    outbox.WithMaxRetries(5),               // 全局默认，默认 3；消息级 MaxRetries 优先（0 = 用默认），下限 1
    outbox.WithPollInterval(10*time.Second), // Start 轮询间隔，默认 5s
    outbox.WithLogger(slog.Default()))       // 默认静默

n, err := d.DispatchPending(ctx)    // 批量分发一次，返回成功投递数
n, err = d.DispatchUntilIdle(ctx)   // 循环分发直到「一轮零投递」——不保证排空：失败消息带重试余量留在 pending
d.Start(ctx)                        // 后台轮询便利模式（库中唯一创建 goroutine 的地方），阻塞至 ctx 取消
```

单条消息的处理顺序：查 Handler（未注册 → 标记 failed）→ 重试上限判定 → 置 processing → 调用 Handler → 成功标记 completed；失败 retries+1，达上限标记 failed 并**跳过继续**（不阻断同批其余消息），未达上限回退 pending。**处理中崩溃的消息停留在 processing，下次拉取无条件重投**（at-least-once，由幂等 Handler 消化）。

### 8.4 Store 契约

库内提供 `InMemoryStore`（内存，测试与进程内用）与 `outbox/filestore`（文件，每消息一文件）。自定义后端实现五个方法，契约要点（ADR-6）：

- `Append`：原子追加；值入参，实现方**拷贝后持有**，并调用 `msg.Normalize()` 补默认字段；ID 重复返回 `ErrDuplicateID`（恢复重试靠它幂等跳过）。
- `FetchPending`：返回 pending **与 processing** 消息（CreatedAt 升序），值为拷贝；processing 无条件重取，无超时/租约机制。
- `MarkProcessed` / `MarkFailed`：幂等——仅「消息不存在」视为成功。
- `UpdateStatus`：原子更新 status+retries+lastError；消息不存在返回错误。
- 实现须支持 `Append` 与 `FetchPending`/状态更新**并发调用**。
- 消息 ID 与 taskID 一样须为文件系统安全字符串。

---

## 9. Saga：多步补偿事务

与引擎无关的独立状态机：每步正向 Action + 逆序 Compensation，任一步失败即逆序补偿已完成步骤。

```go
order := &saga.Saga{
    Name: "order",
    Steps: []saga.SagaStep{
        {
            Name: "reserve",
            Action: func(ctx context.Context, ac saga.ActionCtx) error {
                ac["reservation"] = reserve(ctx) // ActionCtx 在步骤间共享；值必须 JSON 可序列化（由业务保证）
                return nil
            },
            Compensation: func(ctx context.Context, ac saga.ActionCtx) error {
                return release(ctx, ac["reservation"]) // nil Compensation = 幂等步骤，直接标记已补偿
            },
            MaxRetries: 2,             // 最大尝试次数（含首次）；0 = 用 Saga.DefaultMaxRetries，解析下限 1
            Timeout:    5 * time.Second, // 单次尝试超时（WithTimeout 施加）；0 = 无超时
        },
        // ...
    },
    DefaultMaxRetries: 3,
}

c := saga.NewSagaCoordinator(saga.NewInMemorySagaStore(), saga.WithSagaLogger(slog.Default()))
st, err := c.Run(ctx, order, "order-1")     // 新建执行；状态已存在则报错
st, err = c.Resume(ctx, order, "order-1")   // 崩溃/取消后续跑；已终态则原样返回（幂等）
```

要点：

- **步骤定义指纹**：Run 时把步骤名序列哈希写入状态；Resume 校验传入定义与持久化指纹一致才恢复——步骤增删/重排即不一致、即拒绝（ADR-4）。变更步骤序列属新 saga（另起 sagaID）；原 saga 以原定义仍可 Resume。
- **重入契约**：崩溃恢复可能重放一次「进行中」步骤的 Action（幂等或能从 ActionCtx 检测已完成）；补偿按步骤状态跳过已补偿项。
- **补偿同样可重试（同一重试链）、可超时（同一 Timeout）**；单个补偿失败记入 `StepErrors` 后继续其余补偿，不阻断。
- **终态三分**，按此分流告警（ADR-9）：

| 终态 | 含义 | 运维动作 |
|---|---|---|
| `completed` | 全部正向成功 | 无 |
| `compensated` | 正向失败、补偿全部成功（已回滚） | 无 |
| `failed` | **任一补偿失败，需人工介入** | 原始失败原因与补偿失败原因都在 `StepErrors`；人工重置步骤状态后可 Resume |

- ctx 取消：以 `context.WithoutCancel` 保存当前状态后返回 `ctx.Err()`，可 Resume 续跑。

---

## 10. 错误判别

- 错误信息以**包名为前缀**；类别哨兵与底层根因通过 `errors.Join` 同时挂载，二者均可判别：

```go
if errors.Is(err, state.ErrSaveFailed) { /* 持久化失败 */ }
if errors.Is(err, os.ErrPermission)    { /* OS 根因 */ }
if errors.Is(err, outbox.ErrDuplicateID) { /* 消息 ID 重复（幂等跳过场景） */ }
```

全库导出哨兵仅三个：`state.ErrSaveFailed`、`state.ErrLoadFailed`、`outbox.ErrDuplicateID`。其余错误按「前缀 + 根因」逐层包装，不预设更多类别哨兵。

---

## 11. 并发模型与调用方义务

**并发形态**：库不创建、不调度 goroutine（唯一例外 `Dispatcher.Start`），一切执行在调用方 goroutine 内同步推进；`context.Context` 是唯一控制面。并发安全靠隔离而不靠锁，三条**单写者前提**由调用方保证：

1. 同一 taskID 同时至多一个 `Run`；
2. 同一 sagaID 同时至多一个协调器实例；
3. 同一 Store 同时至多一个 `Dispatcher`。

多任务并行天然支持：每任务一个 goroutine + 一个引擎实例 + 一个 taskID，状态按 taskID 隔离。`HandlerRegistry` 内部加锁（Register/Get 可并发），但分发开始后动态更换同 ID Handler 须自行串行化。

**调用方义务清单**（违反即数据错乱或语义破坏）：

- [ ] Execute 的物理副作用幂等或可对齐——快照只保护框架状态，不保护外部系统
- [ ] 不可逆操作经 outbox / saga，不进 Execute
- [ ] 跨进程恢复以相同三元组重建；一产物路径一世系
- [ ] `Cleanup` 在终态 checkpoint 落盘后调用，返回错误不静默
- [ ] Handler / Action / Compensation 幂等且响应 ctx 取消
- [ ] `Accumulator.Add`、意图合并等构造路径的错误不静默丢弃
- [ ] 多实例/多进程写同一状态或消息目录：不支持

---

## 12. 默认值速查

| 项 | 默认值 |
|---|---|
| 管道 batchSize / ingest 批大小 / export pageSize | 1000 |
| producer 每批记录数（outbox） | 1000 |
| ChunkedSink 每块批数 | 10 |
| ChunkedSink 行尾符 / 文件权限 | `\n` / 0644（目录 0755） |
| Dispatcher maxRetries / pollInterval | 3 / 5s（消息级 MaxRetries 优先；解析下限 1） |
| 引擎重试（WithRetry） | 不重试（0）；负值参数 panic |
| Saga 步骤重试解析链 | step → saga.DefaultMaxRetries → 1（下限 1） |
| 日志 | 静默（`outbox.WithLogger` / `saga.WithSagaLogger` 注入；`*log/slog.Logger` 满足接口） |
| 状态 / 消息 / 分块文件 | `<taskID>.state` / `<msgID>.msg.json` / `<name>.chunk_<N>.tmp`、`<name>.final.tmp` |

---

## 13. 示例索引

可运行示例见 [examples/](../examples/)（独立模块，在 examples 目录下执行）：

| 场景 | 命令 | 演示 |
|---|---|---|
| 自定义引擎 | `go run ./custom-engine` | 最小引擎、可重试错误不落盘、checkpoint 恢复、优雅失败 |
| 分页导出 | `go run ./export` | 断点续传、截断重放、绑定指纹反例、Cleanup 协议顺序 |
| JSONL 导入 | `go run ./ingest` | 源偏移补偿、幂等目标消化重放、绑定指纹 |
| Outbox | `go run ./outbox` | producer 骨架产意图（业务数据派生、幂等续收）、RunWithOutbox 幂等对齐、DispatchUntilIdle 不排空 |
| Saga | `go run ./saga` | 补偿事务、终态三分、指纹校验与 Resume 续跑 |
