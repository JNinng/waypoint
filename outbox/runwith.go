package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
)

// RunWithOutbox 是桥接入口：运行引擎并对齐意图到 Store（恢复契约的
// 唯一实现）。对齐使用 Run 返回的最终状态，不二次 Load/反序列化。
//
// 流程：engine.Run 成功后，从最终状态的 outbox 命名空间与 Store 已有
// 消息对齐——缺失则幂等 Append（ErrDuplicateID 即跳过）。这覆盖
// 「Run 成功但 Append 前崩溃」的窗口：重调 RunWithOutbox 幂等恢复。
//
// 对齐完成后分发由调用方的 Dispatcher 完成（Handler 注册表属调用方）。
func RunWithOutbox(ctx context.Context, repo state.StateRepository, eng engine.Engine,
	taskID string, store Store, opts ...engine.RunOption) (*state.BaseTaskState, error) {

	final, err := engine.Run(ctx, repo, eng, taskID, opts...)
	if err != nil {
		return nil, err
	}
	msgs, err := messagesFromPayload(final.Payload)
	if err != nil {
		return final, err
	}
	for _, msg := range msgs {
		if err := store.Append(ctx, msg); err != nil && !errors.Is(err, ErrDuplicateID) {
			return final, fmt.Errorf("outbox: append %s: %w", msg.ID, err)
		}
	}
	return final, nil
}

// messagesFromPayload 解析 Payload 的 outbox 命名空间；
// 缺失该命名空间返回 nil。
func messagesFromPayload(payload json.RawMessage) ([]Message, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		return nil, fmt.Errorf("outbox: parse task payload: %w", err)
	}
	raw, ok := top["outbox"]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var msgs []Message
	if err := json.Unmarshal(raw, &msgs); err != nil {
		return nil, fmt.Errorf("outbox: parse outbox namespace: %w", err)
	}
	return msgs, nil
}
