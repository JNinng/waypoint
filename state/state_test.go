package state

import (
	"encoding/json"
	"errors"
	"testing"
)

// 约束 §8-2：每个哨兵至少被一条 errors.Is 判别测试锁定。
func TestSentinelErrorsAreDiscriminable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"ErrSaveFailed", ErrSaveFailed},
		{"ErrLoadFailed", ErrLoadFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, tc.err) {
				t.Fatalf("errors.Is(%v, %v) = false", tc.err, tc.err)
			}
			other := ErrSaveFailed
			if tc.err == ErrSaveFailed {
				other = ErrLoadFailed
			}
			if errors.Is(tc.err, other) {
				t.Fatalf("%v 不得与兄弟哨兵判别重叠", tc.err)
			}
		})
	}
}

// 设计 §3：BaseTaskState 的 JSON 字段名是持久化契约的一部分。
func TestBaseTaskStateJSONShape(t *testing.T) {
	s := BaseTaskState{
		TaskID:        "t1",
		TaskType:      "export",
		Phase:         PhaseRunning,
		Progress:      42,
		CheckpointLSN: 99,
		Payload:       json.RawMessage(`{"pipeline":{"count":1}}`),
	}
	data, err := json.Marshal(&s)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"task_id":"t1","task_type":"export","phase":"running","progress":42,"checkpoint_lsn":99,"payload":{"pipeline":{"count":1}}}`
	if string(data) != want {
		t.Fatalf("JSON 形状不符合契约:\n got  %s\n want %s", data, want)
	}

	var back BaseTaskState
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Phase != PhaseRunning || back.CheckpointLSN != 99 || back.TaskType != "export" {
		t.Fatalf("round-trip 丢失字段: %+v", back)
	}
}
