package export

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
)

// 绑定指纹三态中的「首 checkpoint 覆盖」：首个 Save（pending→running 步）
// 成功、第二个业务步的 Save 失败 → 恢复时指纹已随首个 checkpoint 落盘，
// fail closed 不得误拒。
func TestExportFingerprintCoversFirstCheckpoint(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "orders.jsonl")
	rows := newRows(20)
	errUpstream := context.DeadlineExceeded             // 任意非 EOF 错误即可
	repo := &memRepo{m: map[string][]byte{}, failOn: 2} // 首个 Save 成功，第二个失败

	eng1 := New(&memSource{rows: rows, failPages: map[int]error{0: errUpstream}}, out, WithPageSize(20))
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望中断")
	}
	// 首个 checkpoint（phase=running）必须已携带指纹。
	data, err := repo.Load(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"export":`) {
		t.Fatalf("首个 checkpoint 缺少绑定指纹命名空间: %s", data)
	}

	// 恢复：指纹一致，不得误拒（解除故障注入）。
	repo.failOn = 0
	eng2 := New(&memSource{rows: rows}, out, WithPageSize(20))
	final, err := engine.Run(context.Background(), repo, eng2, "t1")
	if err != nil {
		t.Fatalf("指纹一致的恢复被误拒: %v", err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
}

// 错配拒绝：同 taskID 换产物路径 → 指纹不一致，拒绝恢复，不落盘、
// 原产物不受影响。反例须在世系未完成时构造（终态 Run 不进 Execute）。
func TestExportBindingMismatchRejected(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "orders.jsonl")
	rows := newRows(20)
	errUpstream := context.DeadlineExceeded // 任意非 EOF 错误即可
	repo := &memRepo{m: map[string][]byte{}, failOn: 2}

	if _, err := engine.Run(context.Background(), repo, New(&memSource{rows: rows, failPages: map[int]error{0: errUpstream}}, out, WithPageSize(20)), "t1"); err == nil {
		t.Fatal("期望中断")
	}
	before, err := repo.Load(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}

	// 同 id、不同产物路径 → 拒绝。
	other := filepath.Join(base, "other.jsonl")
	_, err = engine.Run(context.Background(), repo, New(&memSource{rows: rows}, other, WithPageSize(20)), "t1")
	if err == nil || !strings.Contains(err.Error(), "binding fingerprint mismatch") {
		t.Fatalf("换产物路径必须被指纹拒绝: %v", err)
	}
	// 不落盘：checkpoint 未被污染，错配侧未产生任何文件。
	after, _ := repo.Load(context.Background(), "t1")
	if string(before) != string(after) {
		t.Fatal("被拒绝的恢复不得改动 checkpoint")
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatalf("错配侧不得产生产物: %v", err)
	}
}

// 缺失即拒（fail closed）：手工构造无指纹命名空间的 checkpoint，
// 恢复必须拒绝（库无历史数据兼容负担）。
func TestExportBindingMissingRejected(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "orders.jsonl")
	repo := &memRepo{m: map[string][]byte{}}
	// 无 export 命名空间的 running 状态（合成生产不可达状态，专测拒绝路径）。
	seed, err := json.Marshal(&state.BaseTaskState{
		TaskID: "t1", TaskType: "export", Phase: "running", CheckpointLSN: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.m["t1"] = seed

	_, err = engine.Run(context.Background(), repo, New(&memSource{rows: newRows(5)}, out, WithPageSize(5)), "t1")
	if err == nil || !strings.Contains(err.Error(), "lacks binding fingerprint") {
		t.Fatalf("缺失指纹必须拒绝恢复: %v", err)
	}
}

// 指纹写入不得破坏其他命名空间（outbox 等自持子块共存）。
func TestExportFingerprintPreservesForeignNamespaces(t *testing.T) {
	base := t.TempDir()
	out := filepath.Join(base, "orders.jsonl")
	repo := &memRepo{m: map[string][]byte{}}
	seed, err := json.Marshal(&state.BaseTaskState{
		TaskID: "t1", TaskType: "export", Phase: "pending",
		Payload: json.RawMessage(`{"custom":{"keep":true}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.m["t1"] = seed

	final, err := engine.Run(context.Background(), repo, New(&memSource{rows: newRows(5)}, out, WithPageSize(5)), "t1")
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(final.Payload, &top); err != nil {
		t.Fatal(err)
	}
	if string(top["custom"]) != `{"keep":true}` {
		t.Fatalf("外部命名空间被破坏: %s", top["custom"])
	}
	if _, ok := top["export"]; !ok {
		t.Fatal("指纹命名空间未写入")
	}
}
