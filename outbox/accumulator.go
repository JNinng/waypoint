package outbox

import (
	"encoding/json"
	"fmt"
)

// Accumulator 在引擎内增量收集消息，序列化进 Payload 的 outbox 命名空间
// （随 checkpoint 原子落盘——意图先持久化后分发）。
//
// 并发边界：随引擎在任务 goroutine 内收集，无并发，不加锁。
type Accumulator struct {
	msgs []Message
}

// Add 追加一条消息（拷贝后持有）；重复 ID 返回 ErrDuplicateID，
// 不得静默丢弃。CreatedAt 零值时补当前时间。
func (a *Accumulator) Add(msg Message) error {
	for _, existing := range a.msgs {
		if existing.ID == msg.ID {
			return fmt.Errorf("outbox: accumulate %s: %w", msg.ID, ErrDuplicateID)
		}
	}
	m := cloneMessage(msg)
	m.Normalize()
	a.msgs = append(a.msgs, m)
	return nil
}

// Messages 返回防御性拷贝，内部切片不外泄。
func (a *Accumulator) Messages() []Message {
	out := make([]Message, len(a.msgs))
	for i, m := range a.msgs {
		out[i] = cloneMessage(m)
	}
	return out
}

// MarshalPayload 序列化为 {"outbox":[...]}；空 → (nil, nil)。
func (a *Accumulator) MarshalPayload() (json.RawMessage, error) {
	if len(a.msgs) == 0 {
		return nil, nil
	}
	out := struct {
		Outbox []Message `json:"outbox"`
	}{Outbox: a.msgs}
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("outbox: marshal payload: %w", err)
	}
	return raw, nil
}

// UnmarshalPayload 从 {"outbox":[...]} 还原；空 data → nil；
// 解析失败返回错误（禁止静默返回空结果）。
func (a *Accumulator) UnmarshalPayload(data json.RawMessage) error {
	if len(data) == 0 {
		return nil
	}
	var in struct {
		Outbox []Message `json:"outbox"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		return fmt.Errorf("outbox: parse payload: %w", err)
	}
	a.msgs = in.Outbox
	return nil
}
