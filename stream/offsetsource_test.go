package stream

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeSource(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "src.jsonl")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// drainAll 读尽源（「最后一批 + io.EOF」同次返回属合法组合，内部消化）。
func drainAll(t *testing.T, src *OffsetSource, batchSize int) ([]string, error) {
	t.Helper()
	var lines []string
	for {
		items, err := src.Next(context.Background(), batchSize)
		for _, it := range items {
			lines = append(lines, string(it))
		}
		if err == io.EOF {
			return lines, nil
		}
		if err != nil {
			return lines, err
		}
	}
}

func assertLines(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("lines = %v, want %v", got, want)
	}
}

func TestOffsetSourceLF(t *testing.T) {
	src := NewOffsetSource(writeSource(t, "a\nbb\nccc\n"))
	got, err := drainAll(t, src, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertLines(t, got, "a", "bb", "ccc")
	if src.Position() != 9 {
		t.Fatalf("offset = %d, want 9（含分隔符累计）", src.Position())
	}
}

func TestOffsetSourceCRLFAndMixed(t *testing.T) {
	src := NewOffsetSource(writeSource(t, "a\r\nb\nc\r\n"))
	got, err := drainAll(t, src, 10)
	if err != nil {
		t.Fatal(err)
	}
	assertLines(t, got, "a", "b", "c")
	// a\r\n(3) + b\n(2) + c\r\n(3) = 8
	if src.Position() != 8 {
		t.Fatalf("offset = %d, want 8", src.Position())
	}
}

func TestOffsetSourceFinalLineWithoutNewline(t *testing.T) {
	src := NewOffsetSource(writeSource(t, "a\nb"))
	items, err := src.Next(context.Background(), 10)
	if err != io.EOF {
		t.Fatalf("末行无换行应同批返回 io.EOF, err = %v", err)
	}
	assertLines(t, []string{string(items[0]), string(items[1])}, "a", "b")
	// 再读：彻底耗尽。
	items, err = src.Next(context.Background(), 10)
	if err != io.EOF || items != nil {
		t.Fatalf("(items, err) = (%v, %v), want (nil, io.EOF)", items, err)
	}
	if src.Position() != 3 {
		t.Fatalf("offset = %d, want 3", src.Position())
	}
}

// 批边界：最后一批恰好在批大小处耗尽时，下一批返回纯 EOF。
func TestOffsetSourceBatchBoundary(t *testing.T) {
	src := NewOffsetSource(writeSource(t, "a\nb\n"))
	items, err := src.Next(context.Background(), 2)
	if err != nil {
		t.Fatalf("批内读满不应 EOF: %v", err)
	}
	assertLines(t, []string{string(items[0]), string(items[1])}, "a", "b")
	items, err = src.Next(context.Background(), 2)
	if err != io.EOF || items != nil {
		t.Fatalf("(items, err) = (%v, %v), want (nil, io.EOF)", items, err)
	}
}

func TestOffsetSourceStaterRoundTrip(t *testing.T) {
	path := writeSource(t, "a\nbb\nccc\ndddd\n")
	src := NewOffsetSource(path)
	if _, err := src.Next(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	raw, err := src.MarshalState()
	if err != nil {
		t.Fatal(err)
	}
	// 进程重建：新源恢复偏移后继续，剩余内容衔接。
	src2 := NewOffsetSource(path)
	if err := src2.UnmarshalState(raw); err != nil {
		t.Fatal(err)
	}
	if src2.Position() != src.Position() {
		t.Fatalf("恢复偏移 = %d, want %d", src2.Position(), src.Position())
	}
	got, err := drainAll(t, src2, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertLines(t, got, "ccc", "dddd")
	// nil 状态 = 回退初始。
	if err := src2.UnmarshalState(nil); err != nil {
		t.Fatal(err)
	}
	if src2.Position() != 0 {
		t.Fatalf("nil 状态应回退初始, offset = %d", src2.Position())
	}
}

func TestOffsetSourceAlign(t *testing.T) {
	path := writeSource(t, "a\nbb\nccc\n")
	src := NewOffsetSource(path)
	ctx := context.Background()
	// 对齐到行边界（"a\n" 之后 = 2）。
	if err := src.Align(ctx, 2); err != nil {
		t.Fatal(err)
	}
	items, err := src.Next(ctx, 10)
	if err != io.EOF {
		t.Fatalf("残余两行应同批返回 io.EOF: %v", err)
	}
	assertLines(t, []string{string(items[0]), string(items[1])}, "bb", "ccc")

	// 校验边界：targetLSN 超过文件大小 → 拒绝。
	if err := src.Align(ctx, 1000); err == nil {
		t.Fatal("超出文件大小必须拒绝恢复")
	}
	// 恰好等于文件大小：合法（EOF 位置）。
	if err := src.Align(ctx, 9); err != nil {
		t.Fatalf("文件大小处对齐应成功: %v", err)
	}
}

// 设计 §5.4.2 失败模式：源文件中途被截断 → Align 拒绝。
func TestOffsetSourceAlignRejectsTruncatedFile(t *testing.T) {
	path := writeSource(t, "a\nbb\nccc\n")
	src := NewOffsetSource(path)
	if _, err := src.Next(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	committed := src.Position() // 3
	// 源文件被替换为更短内容。
	if err := os.WriteFile(path, []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := src.Align(context.Background(), committed); err == nil {
		t.Fatal("源被截断后必须拒绝恢复")
	}
}

// 偏移按含分隔符原始字节累计的探针：多字节 UTF-8 行。
func TestOffsetSourceMultibyteLines(t *testing.T) {
	content := "你好\n世界\n"
	src := NewOffsetSource(writeSource(t, content))
	got, err := drainAll(t, src, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertLines(t, got, "你好", "世界")
	if src.Position() != int64(len(content)) {
		t.Fatalf("offset = %d, want %d", src.Position(), len(content))
	}
}
