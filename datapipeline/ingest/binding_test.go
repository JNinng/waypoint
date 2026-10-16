package ingest

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

// 「首 checkpoint 覆盖」回归锚点：首个 Save 成功、第二个业务步的 Save
// 失败 → 恢复时指纹已在盘上，fail closed 不得误拒。
func TestIngestFingerprintCoversFirstCheckpoint(t *testing.T) {
	content, _ := jsonlLines(10)
	src := writeJSONL(t, content)
	repo := &memRepo{m: map[string][]byte{}, failOn: 2}
	target := newMemTarget()

	eng1 := New(src, target, WithBatchSize(5))
	if _, err := engine.Run(context.Background(), repo, eng1, "t1"); err == nil {
		t.Fatal("期望中断")
	}
	data, err := repo.Load(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"ingest":`) {
		t.Fatalf("首个 checkpoint 缺少绑定指纹命名空间: %s", data)
	}

	repo.failOn = 0
	final, err := engine.Run(context.Background(), repo, New(src, target, WithBatchSize(5)), "t1")
	if err != nil {
		t.Fatalf("指纹一致的恢复被误拒: %v", err)
	}
	if final.Phase != "completed" {
		t.Fatalf("phase = %q", final.Phase)
	}
}

// 错配拒绝：同 taskID 换源文件路径 → 指纹不一致，拒绝恢复，不落盘。
// 世系未完成时构造（终态 Run 不进 Execute，无从校验）。
func TestIngestBindingMismatchRejected(t *testing.T) {
	content, _ := jsonlLines(10)
	src := writeJSONL(t, content)
	repo := &memRepo{m: map[string][]byte{}, failOn: 2}
	target := newMemTarget()

	if _, err := engine.Run(context.Background(), repo, New(src, target, WithBatchSize(5)), "t1"); err == nil {
		t.Fatal("期望中断")
	}
	before, err := repo.Load(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}

	// 另一个内容相同但路径不同的源文件。
	otherSrc := filepath.Join(filepath.Dir(src), "other.jsonl")
	if err := os.WriteFile(otherSrc, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), repo, New(otherSrc, target, WithBatchSize(5)), "t1")
	if err == nil || !strings.Contains(err.Error(), "binding fingerprint mismatch") {
		t.Fatalf("换源路径必须被指纹拒绝: %v", err)
	}
	after, _ := repo.Load(context.Background(), "t1")
	if string(before) != string(after) {
		t.Fatal("被拒绝的恢复不得改动 checkpoint")
	}
}

// 缺失即拒（fail closed）。
func TestIngestBindingMissingRejected(t *testing.T) {
	content, _ := jsonlLines(5)
	src := writeJSONL(t, content)
	repo := &memRepo{m: map[string][]byte{}}
	seed, err := json.Marshal(&state.BaseTaskState{
		TaskID: "t1", TaskType: "ingest", Phase: "running", CheckpointLSN: 13,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.m["t1"] = seed

	_, err = engine.Run(context.Background(), repo, New(src, newMemTarget(), WithBatchSize(5)), "t1")
	if err == nil || !strings.Contains(err.Error(), "lacks binding fingerprint") {
		t.Fatalf("缺失指纹必须拒绝恢复: %v", err)
	}
}
