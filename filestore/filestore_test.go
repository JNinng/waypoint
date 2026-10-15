package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jninng/waypoint/state"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Load 缺失返回 (nil, nil)。
	data, err := repo.Load(ctx, "t1")
	if err != nil || data != nil {
		t.Fatalf("Load 缺失 = (%v, %v), want (nil, nil)", data, err)
	}

	if err := repo.Save(ctx, "t1", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	data, err = repo.Load(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"a":1}` {
		t.Fatalf("got %s", data)
	}

	// 全量替换。
	if err := repo.Save(ctx, "t1", []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	data, _ = repo.Load(ctx, "t1")
	if string(data) != `{"a":2}` {
		t.Fatalf("Save 必须全量替换: got %s", data)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	repo, _ := New(dir)
	ctx := context.Background()
	if err := repo.Delete(ctx, "t1"); err != nil {
		t.Fatalf("删除不存在的任务必须成功: %v", err)
	}
	if err := repo.Save(ctx, "t1", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(ctx, "t1"); err != nil {
		t.Fatal(err)
	}
	if data, err := repo.Load(ctx, "t1"); err != nil || data != nil {
		t.Fatalf("删除后 Load = (%v, %v)", data, err)
	}
}

// 设计 §5.1 不变量 4：taskID 必须是文件系统安全字符串。
func TestTaskIDValidation(t *testing.T) {
	repo, _ := New(t.TempDir())
	ctx := context.Background()
	for _, bad := range []string{"", "..", "a/b", "a\\b"} {
		if err := repo.Save(ctx, bad, []byte("x")); err == nil {
			t.Errorf("Save(%q) 应拒绝", bad)
		}
		if _, err := repo.Load(ctx, bad); err == nil {
			t.Errorf("Load(%q) 应拒绝", bad)
		}
		if err := repo.Delete(ctx, bad); err == nil {
			t.Errorf("Delete(%q) 应拒绝", bad)
		}
	}
}

// 约束 §8-2：filestore 错误同时可判别哨兵与 OS 根因。
func TestSaveErrorDiscriminatesSentinelAndCause(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "states")
	repo, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 目录被移除后 Save 失败：哨兵与 os.ErrNotExist 均可达。
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	err = repo.Save(context.Background(), "t1", []byte("x"))
	if err == nil {
		t.Fatal("期望 Save 失败")
	}
	if !errors.Is(err, state.ErrSaveFailed) {
		t.Fatalf("errors.Is(err, state.ErrSaveFailed) = false: %v", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("errors.Is(err, os.ErrNotExist) = false: %v", err)
	}
}

func TestNewCleansCrashTmpResidue(t *testing.T) {
	dir := t.TempDir()
	residue := filepath.Join(dir, "t1.state.tmp")
	if err := os.WriteFile(residue, []byte("half-written"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(residue); !os.IsNotExist(err) {
		t.Fatalf("构造时应清理残留 tmp: %v", err)
	}
}

func TestLoadErrorDiscriminatesSentinelAndCause(t *testing.T) {
	dir := t.TempDir()
	repo, _ := New(dir)
	path := filepath.Join(dir, "t1.state")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 把状态文件替换为目录 → 读取失败（不是 NotExist）。
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	_, err := repo.Load(context.Background(), "t1")
	if err == nil {
		t.Fatal("期望 Load 失败")
	}
	if !errors.Is(err, state.ErrLoadFailed) {
		t.Fatalf("errors.Is(err, state.ErrLoadFailed) = false: %v", err)
	}
	if !strings.Contains(err.Error(), "filestore: load t1") {
		t.Fatalf("错误前缀应为包名: %v", err)
	}
}
