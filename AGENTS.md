# AGENTS.md

面向 agent 的仓库导航与工作约束。设计与决策以 [docs/design.md](docs/design.md)（设计规格）、[docs/adr.md](docs/adr.md)（ADR）、[docs/checklists.md](docs/checklists.md)（待确认与验收清单）为准；本文件只回答「在这里怎么干活」。

## 命令

仓库有三个模块（`datapipeline` 与 `examples` 是嵌套在根模块目录树内的
独立模块，`./...` 模式会跳过嵌套模块，必须分别运行）：

```bash
go build ./... && go vet ./... && go test ./...    # 核心模块
cd datapipeline && go test ./...                   # 扩展模块
cd examples && go build ./... && go vet ./...      # 示例（改动导出 API 后必须编译校验）
```

`go.work` 被 .gitignore 忽略；本地可自建（`go work use . ./datapipeline ./examples`），
无 workspace 时上述命令依然成立（各模块 replace => ../ 兜底解析）。
无外部依赖、无代码生成步骤；Go 1.26+。

## 模块与依赖方向

- 根模块 `github.com/jninng/waypoint`：`state` / `engine` / `stream` / `filestore` / `outbox`（含 `outbox/filestore`）/ `saga` / `internal/*`。
- 扩展模块 `github.com/jninng/waypoint/datapipeline`（`replace => ../`）：`export` / `ingest`。
- **依赖必须单向**：`datapipeline → 核心`；`engine → state`；`stream → engine+state`；`outbox`/`saga` 不依赖 `engine`（唯一例外：`outbox.RunWithOutbox` 桥接）。核心不得反向依赖 datapipeline。
- `internal/atomicfile` 是全库唯一原子写实现（ADR-2）：任何持久化组件不得私写 tmp→Sync→Rename 逻辑。

## 修改代码时的硬约束（完整清单见设计 §8，共 30 条）

高频踩雷点：

1. 失败语义（ADR-1）：`Execute` 返回 error 不落盘；优雅失败（引擎置 `PhaseFailed` 返回 nil）是唯一落盘失败路径；框架不兜底写终态。
2. 重试以快照为起点（ADR-5）：`engine.Run` 在每次 Execute 前深快照、失败回滚；`stream.Pipeline` 每次 Execute 全量还原 Stater——不要引入绕过该机制的增量状态。
3. 字节偏移按含行分隔符的原始字节数累计（禁止会剥离 `\r`/换行的扫描器计算断点）。
4. 中间产物清理：`Cleanup` 由调用方在终态 checkpoint 保存后显式调用，返回 error 不得静默（ADR-3）。
5. 错误包装同时保留哨兵与根因（`errors.Join`）；每个哨兵必须有 `errors.Is` 测试。
6. 幂等方法只把「目标不存在」视为成功；IO 故障必须返回错误。
7. 库代码不得向标准输出打印；日志经注入接口、默认静默。
8. 导出面收敛（设计 §7.1）；测试专用 API 不进生产导出面。

## 测试基线

- 崩溃窗口探针（「Execute/Write 成功但 checkpoint 未保存」）用「第 N 次 Save 注入失败 + 全新组件重跑」模拟；导出与导入链路都必须覆盖，导出断言最终文件逐字节等于源数据。
- 涉及文件句柄的组件不要跨调用持有打开文件（Windows 下 TempDir 清理会失败）。
- 修复提交必须同时包含代码、回归测试与受影响的文档断言（设计 §8 约束 30）。

## Git

- 提交信息只描述变更内容本身，使用约定式前缀（feat/fix/docs/chore/refactor/test），中文描述。
- 当前工作分支：`master`。
