package outbox

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jninng/waypoint/state"
)

// Collector 是 outbox 生产引擎的装配器：组合意图收集与 Payload 的 outbox
// 命名空间装配，把「还原已落盘意图 → 收集 → 合并回命名空间」的正确顺序
// 固化为每业务步的一对括号调用（ADR-11）。
//
// 每业务步的标准用法（Execute 内）：
//
//	if err := c.Restore(s); err != nil { return 0, err } // 步首：还原已落盘意图
//	// ... 业务副作用 + c.Add(意图) ...
//	if err := c.Flush(s); err != nil { return 0, err }   // 步末：合并回 Payload
//
// 跳过 Restore 直接 Flush，会以「只有新意图」的视图覆盖命名空间，丢失已
// 随 checkpoint 落盘的意图。
//
// 与 Accumulator 的分工：Accumulator 是严格收集原语（重复 ID 返回
// ErrDuplicateID）；Collector 在其上固化装配顺序，并对重放收集幂等。
//
// 并发边界：随引擎在任务 goroutine 内使用，无并发（同 Accumulator）。
type Collector struct {
	acc      Accumulator
	restored bool
}

// Restore 把已随 checkpoint 落盘的意图（Payload 的 outbox 命名空间）还原进
// 收集器。每实例只需生效一次：首次调用生效，其后调用为空操作（不清空在册
// 意图）——推荐在每业务步开头无条件调用；须先于首次 Add。
func (c *Collector) Restore(s *state.BaseTaskState) error {
	if c.restored {
		return nil
	}
	ns, err := outboxNamespace(s.Payload)
	if err != nil {
		return err
	}
	if len(ns) > 0 {
		// UnmarshalPayload 的入参是 MarshalPayload 的包裹形态 {"outbox":[...]}，
		// 而命名空间内存放的是消息数组——先包回包裹形态。
		wrapped, err := json.Marshal(map[string]json.RawMessage{"outbox": ns})
		if err != nil {
			return fmt.Errorf("outbox: wrap namespace: %w", err)
		}
		if err := c.acc.UnmarshalPayload(wrapped); err != nil {
			return err
		}
	}
	c.restored = true
	return nil
}

// Add 收集一条意图（拷贝后持有；CreatedAt 零值补当前时间）。对重放收集
// 幂等：意图已在册（同实例重放同一批）视为成功——循 ADR-6 的幂等便捷
// 语义；其他错误原样返回，不静默（约束 §8-18）。
func (c *Collector) Add(msg Message) error {
	if err := c.acc.Add(msg); err != nil && !errors.Is(err, ErrDuplicateID) {
		return err
	}
	return nil
}

// Flush 把收集器的全量视图合并进 Payload 的 outbox 命名空间（保留其他
// 命名空间不动）。收集器为空时不写命名空间。随后的框架 Save 使意图与
// 业务进度同 checkpoint 持久化。
func (c *Collector) Flush(s *state.BaseTaskState) error {
	raw, err := c.acc.MarshalPayload()
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	return mergeOutboxNamespace(s, raw)
}

// outboxNamespace 取任务 Payload 顶层的 outbox 命名空间；缺失返回 nil。
func outboxNamespace(payload json.RawMessage) (json.RawMessage, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		return nil, fmt.Errorf("outbox: parse task payload: %w", err)
	}
	return top["outbox"], nil
}

// mergeOutboxNamespace 把 {"outbox":[...]} 命名空间块合并进 Payload 顶层，
// 保留其他命名空间。
func mergeOutboxNamespace(s *state.BaseTaskState, nsRaw json.RawMessage) error {
	top := map[string]json.RawMessage{}
	if len(s.Payload) > 0 {
		if err := json.Unmarshal(s.Payload, &top); err != nil {
			return fmt.Errorf("outbox: parse task payload: %w", err)
		}
		if top == nil {
			top = map[string]json.RawMessage{}
		}
	}
	var ns map[string]json.RawMessage
	if err := json.Unmarshal(nsRaw, &ns); err != nil {
		return fmt.Errorf("outbox: parse outbox namespace: %w", err)
	}
	top["outbox"] = ns["outbox"]
	out, err := json.Marshal(top)
	if err != nil {
		return fmt.Errorf("outbox: marshal task payload: %w", err)
	}
	s.Payload = out
	return nil
}
