// Package filestore 是 state.StateRepository 的文件后端：
// 每任务一文件，命名 <taskID>.state / <taskID>.state.tmp。
//
// 持久化语义：进程崩溃安全（tmp → Sync → Rename，默认保证）；
// 掉电安全（父目录 fsync）为可选启用项，默认关闭——目录 fsync 在各平台的
// 行为验证后才会提供启用入口（见 docs/checklists.md 待确认 #1）。
//
// 并发边界：多任务可并发访问（按 taskID 隔离到不同文件）；同一 taskID
// 的并发 Save 不受保护（单任务单执行器前提，设计 §2.4）。
package filestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jninng/waypoint/internal/atomicfile"
	"github.com/jninng/waypoint/internal/safename"
	"github.com/jninng/waypoint/state"
)

const (
	dirPerm     os.FileMode = 0o755
	filePerm    os.FileMode = 0o644
	tmpSuffix               = ".state.tmp"
	stateSuffix             = ".state"
)

// FileRepository 把任务状态持久化为目录下的 <taskID>.state 文件。
type FileRepository struct {
	dir string
}

// New 创建文件后端：MkdirAll 0755，并清理崩溃残留的 *.state.tmp。
func New(dir string) (*FileRepository, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("filestore: create dir %s: %w", dir, err)
	}
	if err := atomicfile.CleanResidue(dir, tmpSuffix); err != nil {
		return nil, fmt.Errorf("filestore: %w", err)
	}
	return &FileRepository{dir: dir}, nil
}

// Load 加载状态；任务不存在返回 (nil, nil)。
func (r *FileRepository) Load(ctx context.Context, taskID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := safename.Validate(taskID); err != nil {
		return nil, fmt.Errorf("filestore: load %s: %w", taskID, err)
	}
	data, err := os.ReadFile(r.path(taskID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("filestore: load %s: %w", taskID, errors.Join(state.ErrLoadFailed, err))
	}
	return data, nil
}

// Save 原子全量替换状态。
func (r *FileRepository) Save(ctx context.Context, taskID string, s []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safename.Validate(taskID); err != nil {
		return fmt.Errorf("filestore: save %s: %w", taskID, err)
	}
	if err := atomicfile.Write(r.path(taskID), s, filePerm); err != nil {
		return fmt.Errorf("filestore: save %s: %w", taskID, errors.Join(state.ErrSaveFailed, err))
	}
	return nil
}

// Delete 幂等删除；删除不存在的任务不报错，IO 故障返回错误。
func (r *FileRepository) Delete(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safename.Validate(taskID); err != nil {
		return fmt.Errorf("filestore: delete %s: %w", taskID, err)
	}
	if err := os.Remove(r.path(taskID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("filestore: delete %s: %w", taskID, err)
	}
	return nil
}

func (r *FileRepository) path(taskID string) string {
	return filepath.Join(r.dir, taskID+stateSuffix)
}
