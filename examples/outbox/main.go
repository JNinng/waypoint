// 示例：outbox 模式——意图先落盘后分发（设计 §6.3 / §5.8、ADR-11/ADR-12）。
//
// 业务场景：到货订单通知。引擎经 outbox/producer 骨架组装：业务方只提供
// 记录源（Next 内做幂等副作用）与意图派生函数；引导步、已落盘意图还原、
// 分批推进、合并落盘、耗尽收尾由组件固定。意图从业务记录派生——消息 ID
// 由订单号稳定生成（重放可识别），Payload 携带订单数据；不可逆的「发送」
// 不在这里发生，由分发层 Handler 执行（设计 §1：不可逆操作不得进引擎
// Execute）。
//
// 三幕：
//  1. 第 2 批第 2 张订单的意图派生失败（模板服务超时）→ Run 失败不落盘；
//     checkpoint 停在第一批，本批已标记、SO-1003 的意图只在本进程收集器。
//  2. 故障解除，同一引擎与 Task 重跑：从 checkpoint 进度续收——重放收集
//     的在册意图由 Collector 幂等消化；跨进程重建时全新收集器经 Restore
//     自动还原已落盘意图，增量收集不丢失。
//  3. RunWithOutbox 把意图幂等对齐到 Store（重复 ID 跳过）；Dispatcher 以
//     at-least-once 分发到幂等 Handler；DispatchUntilIdle 演示「无进展即停」：
//     一张订单的通知连续两次「服务商限流」后停止——不排空，失败消息带重试
//     余量留在 pending，再次分发才投出。
//
// 运行：go run ./outbox
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/outbox/producer"
	"github.com/jninng/waypoint/state"
	"github.com/jninng/waypoint/waypoint"
)

const (
	batchSize    = 2 // 每批订单数（producer 骨架的批大小）
	deriveFailID = "SO-1004"
	flakyID      = "notify-SO-1005"
	flakyTries   = 3
)

// order 是一条到货业务记录（生产中来自上游系统或任务输入）。
type order struct {
	id    string
	email string
}

// orderSource 是切片型记录源：按 after（已提交记录数，即引擎 LSN）定位本批。
// 仓库侧「已入待通知队列」标记是幂等副作用，在 Next 内完成——先副作用后
// 意图收集；重放同一批不累积。
type orderSource struct {
	orders []order
	queued map[string]bool
}

func (s *orderSource) Next(_ context.Context, after int64, batchSize int) ([]order, error) {
	start := int(after)
	if start >= len(s.orders) {
		return nil, io.EOF
	}
	end := min(start+batchSize, len(s.orders))
	for _, o := range s.orders[start:end] {
		s.queued[o.id] = true
	}
	if end >= len(s.orders) {
		return s.orders[start:end], io.EOF // 末批与 EOF 同次返回
	}
	return s.orders[start:end], nil
}

func main() {
	dir, err := os.MkdirTemp("", "waypoint-outbox-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	stateDir := filepath.Join(dir, "states")
	store := outbox.NewInMemoryStore() // 生产可换 outbox/filestore 或自实现 Store
	ctx := context.Background()

	orders := []order{
		{"SO-1001", "ada@dev.io"},
		{"SO-1002", "grace@dev.io"},
		{"SO-1003", "linus@dev.io"},
		{"SO-1004", "margaret@dev.io"},
		{"SO-1005", "ken@dev.io"},
		{"SO-1006", "barbara@dev.io"},
	}

	fmt.Println("幕一：第 2 批第 2 张订单派生失败 → Run 失败不落盘")
	// 意图派生从业务记录派生消息（ID = "notify-" + 订单号，稳定、重放可
	// 识别）；注入瞬态故障：SO-1004 首次派生失败——此时 SO-1003 的意图已在
	// 收集器、仓库标记已完成，但 checkpoint 未推进。
	healed := false
	derive := func(_ context.Context, taskID string, o order) (outbox.Message, error) {
		if !healed && o.id == deriveFailID {
			return outbox.Message{}, errors.New("通知模板服务超时")
		}
		payload, err := json.Marshal(map[string]string{"order": o.id, "email": o.email})
		if err != nil {
			return outbox.Message{}, err
		}
		return outbox.Message{
			ID:        "notify-" + o.id,
			TaskID:    taskID,
			EventType: "arrival-notify",
			Payload:   payload,
		}, nil
	}

	// 一个引擎 + 一个 Task 贯穿全例：收集器随实例延续，跨 Run 的重放收集
	// 由 Collector 幂等消化；跨进程重建以相同三元组重建 Task（全新收集器
	// 由骨架内的 Restore 还原已落盘意图），两条路径同一份代码。
	eng := producer.New("arrival-notify",
		&orderSource{orders: orders, queued: map[string]bool{}},
		derive, producer.WithBatchSize[order](batchSize))
	task := waypoint.NewTask(stateDir, eng, "notify-42")

	if _, err := task.Run(ctx); err != nil {
		fmt.Printf("  第一次 Run 失败: %v\n", err)
	}
	printCheckpoint(stateDir, "notify-42", len(orders))

	fmt.Println("幕二：故障解除 → 同一 Task 重跑，从 checkpoint 续收")
	healed = true
	final, err := task.Run(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("  第二次 Run 完成: phase=%s checkpoint_lsn=%d\n", final.Phase, final.CheckpointLSN)

	// Run 成功后把意图幂等对齐到 Store；重复 ID（恢复重试）跳过。
	if _, err := task.RunWithOutbox(ctx, store); err != nil {
		log.Fatal(err)
	}
	pending, _ := store.FetchPending(ctx)
	fmt.Printf("  RunWithOutbox 对齐: %d 条意图进入 Store\n", len(pending))

	fmt.Println("幕三：分发（at-least-once）")
	// 重调同一 Task：终态直接返回、Append 撞重复 ID 幂等跳过——不重复投递。
	if _, err := task.RunWithOutbox(ctx, store); err != nil {
		log.Fatal(err)
	}
	pending, _ = store.FetchPending(ctx)
	fmt.Printf("  重调 RunWithOutbox 幂等: pending=%d 条\n", len(pending))

	// Handler 必须幂等（同一条消息可能被投递多次）；SO-1005 的通知前两次
	// 「服务商限流」——DispatchUntilIdle 在无进展的轮次停止（不排空）。
	sent := map[string]int{}
	registry := outbox.NewHandlerRegistry(map[string]outbox.Handler{
		"arrival-notify": func(ctx context.Context, m outbox.Message) error {
			sent[m.ID]++
			if m.ID == flakyID && sent[m.ID] < flakyTries {
				return errors.New("邮件服务商限流")
			}
			fmt.Printf("  到货通知已发送: %s → %s\n", m.ID, m.Payload)
			return nil
		},
	})
	dispatcher := outbox.NewDispatcher(store, registry)

	delivered, err := dispatcher.DispatchUntilIdle(ctx)
	if err != nil {
		log.Fatal(err)
	}
	pending, _ = store.FetchPending(ctx)
	fmt.Printf("  DispatchUntilIdle: 投递 %d 条，剩余 pending=%d（无进展即停，不排空）\n",
		delivered, len(pending))

	// 再次分发：SO-1005 第 3 次尝试成功，全部投递完成。
	delivered, err = dispatcher.DispatchPending(ctx)
	if err != nil {
		log.Fatal(err)
	}
	pending, _ = store.FetchPending(ctx)
	fmt.Printf("  再次分发: 投递 %d 条，剩余 pending=%d（at-least-once，重放由幂等 Handler 消化）\n",
		delivered, len(pending))
}

// printCheckpoint 直接读状态文件（Task 隐藏了 repo，演示用直读），
// 并统计已随 checkpoint 落盘的意图数。
func printCheckpoint(stateDir, taskID string, total int) {
	data, err := os.ReadFile(filepath.Join(stateDir, taskID+".state"))
	if err != nil {
		log.Fatal(err)
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		log.Fatal(err)
	}
	intents := 0
	if len(s.Payload) > 0 {
		var top map[string]json.RawMessage
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			log.Fatal(err)
		}
		if raw := top["outbox"]; len(raw) > 0 {
			var msgs []outbox.Message
			if err := json.Unmarshal(raw, &msgs); err != nil {
				log.Fatal(err)
			}
			intents = len(msgs)
		}
	}
	fmt.Printf("  checkpoint: phase=%s checkpoint_lsn=%d（已提交 %d/%d 张订单）已落盘意图=%d 条\n",
		s.Phase, s.CheckpointLSN, s.CheckpointLSN, total, intents)
}
