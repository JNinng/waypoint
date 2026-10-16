// Package atomicfile 提供全库唯一的原子写原语：tmp → Sync → Rename。
//
// 持久化语义分级：
//   - 进程崩溃安全（默认保证，Write/Save）：中途崩溃只残留临时文件，
//     目标文件要么完整出现、要么完全不出现，由各后端在构造入口清理残留。
//   - 掉电安全（可选，WriteDurably）：Rename 后对父目录 fsync（POSIX
//     open+fsync；Windows 目录句柄 FlushFileBuffers）。目录 fsync 在
//     Windows 上的实际行为未实测（见 docs/checklists.md 待确认 #1），
//     启用方须自行验证。
//
// 任何持久化组件不得私写临时文件逻辑，一律复用本包（ADR-2）。
package atomicfile

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Write 以进程崩溃安全语义原子写入 data 到 path：
// 写 <path>.tmp → Sync → Rename。perm 仅作用于新建文件。
// 任意一步失败都会删除临时文件并返回错误。
func Write(path string, data []byte, perm fs.FileMode) error {
	_, err := write(path, path+".tmp", bytes.NewReader(data), perm, false)
	return err
}

// WriteDurably 在 Write 语义之上追加父目录 fsync（掉电安全，可选启用）。
func WriteDurably(path string, data []byte, perm fs.FileMode) error {
	_, err := write(path, path+".tmp", bytes.NewReader(data), perm, true)
	return err
}

// Save 从 r 流式写入 tmpPath，Sync 后 Rename 为 path。
// tmpPath 显式指定（如 <name>.final.tmp），失败时清理 tmpPath。
// 返回写入的字节数。
func Save(path, tmpPath string, r io.Reader, perm fs.FileMode) (int64, error) {
	return write(path, tmpPath, r, perm, false)
}

// CleanResidue 删除 dir 下文件名以 tmpSuffix 结尾的崩溃残留临时文件。
// 各持久化后端在构造入口调用（构造入口自动清理残留 tmp 的统一实现）。
func CleanResidue(dir, tmpSuffix string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("atomicfile: read dir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), tmpSuffix) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("atomicfile: clean tmp %s: %w", e.Name(), err)
		}
	}
	return nil
}

func write(path, tmpPath string, r io.Reader, perm fs.FileMode, fsyncDir bool) (int64, error) {
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return 0, fmt.Errorf("atomicfile: create %s: %w", tmpPath, err)
	}
	n, err := io.Copy(f, r)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return n, fmt.Errorf("atomicfile: write %s: %w", tmpPath, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return n, fmt.Errorf("atomicfile: sync %s: %w", tmpPath, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return n, fmt.Errorf("atomicfile: close %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return n, fmt.Errorf("atomicfile: rename %s -> %s: %w", tmpPath, path, err)
	}
	if fsyncDir {
		if err := syncDir(filepath.Dir(path)); err != nil {
			return n, fmt.Errorf("atomicfile: sync dir of %s: %w", path, err)
		}
	}
	return n, nil
}

// syncDir 对父目录执行 fsync。目录句柄以只读方式打开——POSIX 上可用；
// Windows 上 FlushFileBuffers 可能对只读目录句柄返回拒绝访问，此时
// 错误如实上抛，由启用方（掉电安全为可选项）决定是否接受。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
