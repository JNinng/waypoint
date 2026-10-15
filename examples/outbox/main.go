// 示例：outbox 模式——意图先落盘后分发（设计 §6.3 / §5.6）。
//
// 业务引擎在 Execute 内收集不可逆操作意图（Accumulator → Payload 的
// outbox 命名空间，随 checkpoint 原子落盘）；RunWithOutbox 把最终状态的
// 意图幂等对齐到 Store（重复 ID 跳过，覆盖「Run 成功但 Append 前崩溃」
// 窗口）；Dispatcher 以 at-least-once 分发到幂等 Handler——传真「线路忙」
// 演示失败回退 pending、下一轮重投。
//
// 运行：go run ./outbox
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/filestore"
	"github.com/jninng/waypoint/outbox"
	"github.com/jninng/waypoint/state"
)

// notifyEngine 通知任务：收集 3 条不可逆操作意图（不直接执行——
// 发邮件/短信/传真不得进引擎 Execute，见设计 §1）。
type notifyEngine struct{ acc *outbox.Accumulator }

func (e *notifyEngine) TaskType() string        { return "notify" }
func (e *notifyEngine) Phases() engine.PhaseSet { return engine.DefaultPhaseSet() }

func (e *notifyEngine) Execute(ctx context.Context, s *state.BaseTaskState) (state.LSN, error) {
	if s.Phase == state.PhasePending {
		s.Phase = state.PhaseRunning
		return 0, nil
	}
	msgs := []outbox.Message{
		{ID: "m1", TaskID: s.TaskID, EventType: "email", Payload: json.RawMessage(`{"to":"ada@dev.io"}`)},
		{ID: "m2", TaskID: s.TaskID, EventType: "sms", Payload: json.RawMessage(`{"to":"+8613800000000"}`)},
		{ID: "m3", TaskID: s.TaskID, EventType: "fax", Payload: json.RawMessage(`{"to":"0571-88880000"}`)},
	}
	for _, m := range msgs {
		if err := e.acc.Add(m); err != nil {
			return 0, err // 重复 ID 等错误不得静默丢弃（约束 §8-18）
		}
	}
	raw, err := e.acc.MarshalPayload() // {"outbox":[...]}
	if err != nil {
		return 0, err
	}
	if err := mergeNamespace(s, raw); err != nil {
		return 0, err
	}
	s.Phase = state.PhaseCompleted
	return 1, nil
}

// mergeNamespace 把 {"outbox":[...]} 命名空间块合并进 Payload 顶层。
// 框架不提供合并助手（设计 §3）：各参与方自持子块，组合引擎负责拼装。
func mergeNamespace(s *state.BaseTaskState, nsRaw json.RawMessage) error {
	top := map[string]json.RawMessage{}
	if len(s.Payload) > 0 {
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			return err
		}
	}
	var ns map[string]json.RawMessage
	if err := json.Unmarshal(nsRaw, &ns); err != nil {
		return err
	}
	top["outbox"] = ns["outbox"]
	out, err := json.Marshal(top)
	if err != nil {
		return err
	}
	s.Payload = out
	return nil
}

func main() {
	dir, err := os.MkdirTemp("", "waypoint-outbox-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	repo, err := filestore.New(filepath.Join(dir, "states"))
	if err != nil {
		log.Fatal(err)
	}
	store := outbox.NewInMemoryStore()
	ctx := context.Background()

	// 意图随 checkpoint 原子落盘，Run 成功后对齐到 Store。
	final, err := outbox.RunWithOutbox(ctx, repo, &notifyEngine{acc: &outbox.Accumulator{}}, "notify-1", store)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("任务完成: phase=%s；意图已对齐到 Store\n", final.Phase)

	// 恢复窗口演示：重调 RunWithOutbox——Run 直接返回 completed 状态，
	// Append 撞重复 ID 幂等跳过，不产生重复消息。
	if _, err := outbox.RunWithOutbox(ctx, repo, &notifyEngine{acc: &outbox.Accumulator{}}, "notify-1", store); err != nil {
		log.Fatal(err)
	}
	pending, _ := store.FetchPending(ctx)
	fmt.Printf("重调 RunWithOutbox 幂等: pending=%d 条\n", len(pending))

	// 分发：注册幂等 Handler；传真第一次「线路忙」→ 回退 pending，下轮重投。
	registry := &outbox.HandlerRegistry{}
	registry.Register("email", func(ctx context.Context, m outbox.Message) error {
		fmt.Printf("  邮件已发送 → %s\n", m.Payload)
		return nil
	})
	registry.Register("sms", func(ctx context.Context, m outbox.Message) error {
		fmt.Printf("  短信已发送 → %s\n", m.Payload)
		return nil
	})
	faxAttempts := 0
	registry.Register("fax", func(ctx context.Context, m outbox.Message) error {
		faxAttempts++
		if faxAttempts == 1 {
			return errors.New("fax line busy")
		}
		fmt.Printf("  传真已发送（第 %d 次尝试）→ %s\n", faxAttempts, m.Payload)
		return nil
	})
	dispatcher := outbox.NewDispatcher(store, registry)

	n1, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		log.Fatal(err)
	}
	n2, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		log.Fatal(err)
	}
	pending, _ = store.FetchPending(ctx)
	fmt.Printf("分发两轮: 投递 %d + %d 条，剩余 pending=%d（at-least-once，重放由幂等 Handler 消化）\n",
		n1, n2, len(pending))
}
