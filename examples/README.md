# examples

各场景的可运行示例，与 [docs/design.md](../docs/design.md) §6 关键流程及各能力机制一一对应。
examples 是独立模块（replace 指向核心库与 datapipeline 扩展包），在本目录下运行：

| 目录 | 场景 | 设计章节 |
|---|---|---|
| `custom-engine` | 最小自定义引擎：可重试错误不落盘、checkpoint 恢复、优雅失败 | §5.2 |
| `export` | 分页导出断点续传：截断重放、Cleanup 协议顺序 | §6.1 |
| `ingest` | JSONL 导入偏移恢复：源偏移补偿、幂等目标消化重放 | §6.2 |
| `outbox` | 意图先落盘后分发：RunWithOutbox 幂等对齐、at-least-once 重投 | §6.3 |
| `saga` | 多步补偿事务：终态三分、指纹校验、Resume 续跑 | §6.4 |

```bash
go run ./custom-engine
go run ./export
go run ./ingest
go run ./outbox
go run ./saga
```
