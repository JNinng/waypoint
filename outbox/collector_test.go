package outbox

import (
	"encoding/json"
	"testing"

	"github.com/jninng/waypoint/state"
)

// collectorPayloadIDs 解析 Payload 的 outbox 命名空间，返回意图 ID → 出现次数。
func collectorPayloadIDs(t *testing.T, s *state.BaseTaskState) map[string]int {
	t.Helper()
	ids := map[string]int{}
	if len(s.Payload) == 0 {
		return ids
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(s.Payload, &top); err != nil {
		t.Fatalf("解析 Payload: %v", err)
	}
	raw := top["outbox"]
	if len(raw) == 0 {
		return ids
	}
	var msgs []Message
	if err := json.Unmarshal(raw, &msgs); err != nil {
		t.Fatalf("解析 outbox 命名空间: %v", err)
	}
	for _, m := range msgs {
		ids[m.ID]++
	}
	return ids
}

// ADR-11：还原一次 + 重放收集幂等 + Flush 合并——同实例重放（恢复/重试）
// 不得产生重复意图，也不得丢失在册意图。
func TestCollectorReplayIdempotent(t *testing.T) {
	var c Collector
	s := &state.BaseTaskState{}

	if err := c.Restore(s); err != nil { // 全新任务：无命名空间可还原
		t.Fatal(err)
	}
	if err := c.Add(Message{ID: "m1", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(s); err != nil {
		t.Fatal(err)
	}
	if ids := collectorPayloadIDs(t, s); len(ids) != 1 {
		t.Fatalf("首次 Flush 后应恰有一条意图: %v", ids)
	}

	// 重放（Payload 保持步骤前视图，收集器仍在册 m1）。
	if err := c.Restore(s); err != nil { // 二次调用为空操作，不得清空在册
		t.Fatal(err)
	}
	if err := c.Add(Message{ID: "m1", TaskID: "t", EventType: "e"}); err != nil { // 在册 → 幂等成功
		t.Fatal(err)
	}
	if err := c.Add(Message{ID: "m2", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(s); err != nil {
		t.Fatal(err)
	}
	ids := collectorPayloadIDs(t, s)
	if len(ids) != 2 || ids["m1"] != 1 || ids["m2"] != 1 {
		t.Fatalf("重放后意图应恰为 m1、m2 各一条: %v", ids)
	}
}

// 跨进程重建：全新收集器 Restore 还原已落盘意图后增量收集，Flush 不丢失。
func TestCollectorRestoreFromCheckpoint(t *testing.T) {
	var first Collector
	saved := &state.BaseTaskState{}
	if err := first.Restore(saved); err != nil {
		t.Fatal(err)
	}
	if err := first.Add(Message{ID: "m1", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Flush(saved); err != nil {
		t.Fatal(err)
	}

	var fresh Collector
	s := &state.BaseTaskState{Payload: append(json.RawMessage(nil), saved.Payload...)}
	if err := fresh.Restore(s); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Add(Message{ID: "m2", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := fresh.Flush(s); err != nil {
		t.Fatal(err)
	}
	ids := collectorPayloadIDs(t, s)
	if len(ids) != 2 || ids["m1"] != 1 || ids["m2"] != 1 {
		t.Fatalf("还原后增量收集不得丢失已落盘意图: %v", ids)
	}
}

// Flush 保留其他命名空间；空收集器不写命名空间。
func TestCollectorFlushNamespaceHygiene(t *testing.T) {
	var c Collector
	s := &state.BaseTaskState{Payload: json.RawMessage(`{"export":{"binding":"x"},"keep":1}`)}
	if err := c.Restore(s); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(s); err != nil { // 空收集器：不写 outbox，不破坏原有命名空间
		t.Fatal(err)
	}
	if string(s.Payload) != `{"export":{"binding":"x"},"keep":1}` {
		t.Fatalf("空 Flush 不得改动 Payload: %s", s.Payload)
	}

	if err := c.Add(Message{ID: "m1", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(s); err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(s.Payload, &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["outbox"]; !ok {
		t.Fatal("Flush 应写入 outbox 命名空间")
	}
	if string(top["export"]) != `{"binding":"x"}` || string(top["keep"]) != "1" {
		t.Fatalf("其他命名空间必须保留: %s", s.Payload)
	}
}

// 非法 Payload → Restore 显式报错（解析失败禁止静默，约束 §8-14）。
func TestCollectorRestoreInvalidPayload(t *testing.T) {
	var c Collector
	s := &state.BaseTaskState{Payload: json.RawMessage(`[1,2]`)}
	if err := c.Restore(s); err == nil {
		t.Fatal("非法 Payload 必须报错")
	}
}

// Add 沿用 Accumulator 契约：拷贝后持有，CreatedAt 零值补当前时间。
func TestCollectorAddNormalizes(t *testing.T) {
	var c Collector
	s := &state.BaseTaskState{}
	if err := c.Restore(s); err != nil {
		t.Fatal(err)
	}
	if err := c.Add(Message{ID: "m1", TaskID: "t", EventType: "e"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(s); err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(s.Payload, &top); err != nil {
		t.Fatal(err)
	}
	var msgs []Message
	if err := json.Unmarshal(top["outbox"], &msgs); err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].ID != "m1" {
		t.Fatalf("意图缺失: %+v", msgs)
	}
	if msgs[0].CreatedAt.IsZero() {
		t.Fatal("CreatedAt 零值应补当前时间")
	}
}
