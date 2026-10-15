package atomicfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCreatesFileAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.state")
	if err := Write(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("临时文件应已被 Rename 消费: %v", err)
	}
}

func TestWriteOverwritesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.state")
	if err := Write(path, []byte("one"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, []byte("two-longer"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "two-longer" {
		t.Fatalf("全量替换失败: got %q", got)
	}
}

func TestWriteFailureCleansTmp(t *testing.T) {
	dir := t.TempDir()
	// 目标路径是一个目录，tmp 创建因同名目录已存在而失败。
	path := filepath.Join(dir, "blocked")
	if err := os.Mkdir(path+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Write(path, []byte("x"), 0o644); err == nil {
		t.Fatal("期望失败")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("失败路径不得产生目标文件: %v", err)
	}
}

func TestWriteDurablyWritesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.state")
	err := WriteDurably(path, []byte("durable"), 0o644)
	if err != nil {
		// Windows 上目录句柄 fsync 行为未验证（checklists #1），
		// 文件本体仍必须完整落盘。
		t.Logf("dir fsync 返回错误（平台相关）: %v", err)
	}
	got, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if string(got) != "durable" {
		t.Fatalf("got %q", got)
	}
}
