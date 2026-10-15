package stream

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func lines(items ...string) [][]byte {
	out := make([][]byte, len(items))
	for i, s := range items {
		out[i] = []byte(s)
	}
	return out
}

func readAllChunks(t *testing.T, dir, base string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".chunk_") && strings.HasSuffix(name, ".tmp") {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			sb.Write(data)
		}
	}
	return sb.String()
}

func countChunks(t *testing.T, dir, base string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, base+".chunk_") && strings.HasSuffix(name, ".tmp") {
			n++
		}
	}
	return n
}

func TestChunkedSinkWriteAndFinalize(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl", WithBatchesPerChunk(2))
	ctx := context.Background()

	if err := sink.Write(ctx, lines("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(ctx, lines("c", "d")); err != nil {
		t.Fatal(err)
	}
	// 每块 2 批：第一次 Write 后未滚动，第二次后滚动到块 1。
	if err := sink.Write(ctx, lines("e")); err != nil {
		t.Fatal(err)
	}
	if got := countChunks(t, dir, "out.jsonl"); got != 2 {
		t.Fatalf("chunks = %d, want 2", got)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := "a\nb\nc\nd\ne\n"
	if string(data) != want {
		t.Fatalf("final = %q, want %q", data, want)
	}
	// Finalize 不删除分块（重放输入）。
	if got := countChunks(t, dir, "out.jsonl"); got != 2 {
		t.Fatalf("Finalize 后 chunks = %d, want 2（不得删除分块）", got)
	}
	// 残留 .final.tmp 不存在。
	if _, err := os.Stat(filepath.Join(dir, "out.jsonl.final.tmp")); !os.IsNotExist(err) {
		t.Fatalf("final.tmp 应已消费: %v", err)
	}
}

// 截断重放：Write 物理成功但 Stater 未推进（模拟 Write 后 Save 前崩溃），
// 重放同一批时截断消除多余字节。
func TestChunkedSinkTruncationReplay(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl")
	ctx := context.Background()

	// 已提交边界：空（无状态）。
	// 物理写入两个批次的字节（模拟崩溃残留）。
	if err := sink.Write(ctx, lines("a")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(ctx, lines("b")); err != nil {
		t.Fatal(err)
	}
	// 回退到已提交状态（无）——等价于崩溃后重建组件。
	if err := sink.UnmarshalState(nil); err != nil {
		t.Fatal(err)
	}
	// 重放第一批：截断必须消除 a、b 的残留。
	if err := sink.Write(ctx, lines("a")); err != nil {
		t.Fatal(err)
	}
	got := readAllChunks(t, dir, "out.jsonl")
	if got != "a\n" {
		t.Fatalf("截断重放失败, chunk = %q, want %q", got, "a\n")
	}
}

// 末行无换行 / 自定义行尾符。
func TestChunkedSinkCustomSuffix(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.txt", WithLineSuffix([]byte("\r\n")))
	if err := sink.Write(context.Background(), lines("x", "y")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "out.txt"))
	if string(data) != "x\r\ny\r\n" {
		t.Fatalf("got %q", data)
	}
}

// Finalize 幂等：重复调用产生相同最终文件。
func TestChunkedSinkFinalizeIdempotent(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl", WithBatchesPerChunk(1))
	ctx := context.Background()
	for _, l := range []string{"a", "b", "c"} {
		if err := sink.Write(ctx, lines(l)); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if string(data) != "a\nb\nc\n" {
		t.Fatalf("got %q", data)
	}
}

// 空导出：无任何写入直接 Finalize → 空最终文件。
func TestChunkedSinkFinalizeEmpty(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl")
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("got %q", data)
	}
}

// Stater 状态往返：恢复后继续写，内容衔接正确。
func TestChunkedSinkStaterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl")
	ctx := context.Background()
	if err := sink.Write(ctx, lines("a", "b")); err != nil {
		t.Fatal(err)
	}
	raw, err := sink.MarshalState()
	if err != nil {
		t.Fatal(err)
	}

	// 模拟进程重建：新组件恢复状态后继续。
	sink2 := NewChunkedSink(dir, "out.jsonl")
	if err := sink2.UnmarshalState(raw); err != nil {
		t.Fatal(err)
	}
	if err := sink2.Write(ctx, lines("c")); err != nil {
		t.Fatal(err)
	}
	if err := sink2.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if string(data) != "a\nb\nc\n" {
		t.Fatalf("got %q", data)
	}
}

// Cleanup 删除分块与残留 final.tmp，但保留最终文件。
func TestChunkedSinkCleanup(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl", WithBatchesPerChunk(1))
	ctx := context.Background()
	if err := sink.Write(ctx, lines("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	// 残留一个假 final.tmp 验证清理。
	if err := os.WriteFile(filepath.Join(dir, "out.jsonl.final.tmp"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sink.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if got := countChunks(t, dir, "out.jsonl"); got != 0 {
		t.Fatalf("cleanup 后 chunks = %d", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.jsonl.final.tmp")); !os.IsNotExist(err) {
		t.Fatal("final.tmp 未清理")
	}
	data, err := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if err != nil || string(data) != "a\nb\n" {
		t.Fatalf("最终文件必须保留: %v %q", err, data)
	}
}

// Cleanup 错误可见：目录不存在时报错。
func TestChunkedSinkCleanupMissingDir(t *testing.T) {
	sink := NewChunkedSink(filepath.Join(t.TempDir(), "gone"), "out.jsonl")
	if err := sink.Cleanup(); err == nil {
		t.Fatal("目录不可读时 Cleanup 必须返回错误")
	}
}

// finalName 含子目录前缀时布局仍正确。
func TestChunkedSinkSubdirFinalName(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, filepath.Join("sub", "out.jsonl"), WithBatchesPerChunk(1))
	ctx := context.Background()
	if err := sink.Write(ctx, lines("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "sub", "out.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "a\nb\n" {
		t.Fatalf("got %q", data)
	}
	if err := sink.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if got := countChunks(t, filepath.Join(dir, "sub"), "out.jsonl"); got != 0 {
		t.Fatalf("子目录分块未清理: %d", got)
	}
}

// 末块字节数边界：Finalize 只拼接已提交字节。
func TestChunkedSinkFinalizeRespectsCommittedBoundary(t *testing.T) {
	dir := t.TempDir()
	sink := NewChunkedSink(dir, "out.jsonl")
	ctx := context.Background()
	if err := sink.Write(ctx, lines("a")); err != nil {
		t.Fatal(err)
	}
	raw, _ := sink.MarshalState()
	// 崩溃残留：再写一批但不推进状态。
	if err := sink.Write(ctx, lines("GHOST")); err != nil {
		t.Fatal(err)
	}
	// 恢复到只含 "a" 的已提交状态后 Finalize：GHOST 不得进入最终文件。
	if err := sink.UnmarshalState(raw); err != nil {
		t.Fatal(err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(dir, "out.jsonl"))
	if !bytes.Equal(data, []byte("a\n")) {
		t.Fatalf("final = %q, want %q", data, "a\n")
	}
}

func TestWithBatchesPerChunkPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("n < 1 必须 panic")
		}
	}()
	WithBatchesPerChunk(0)
}
