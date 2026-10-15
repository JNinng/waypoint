// Package filestore 是 outbox.Store 的文件后端：每消息一文件，
// 命名 <msgID>.msg.json / <msgID>.msg.json.tmp，原子写经
// internal/atomicfile（全库唯一实现）。
//
// 并发边界：按消息一文件 + 原子重命名天然隔离——支持 Append 与
// FetchPending/状态更新并发调用；多进程访问同一目录不在支持范围内。
package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jninng/waypoint/internal/atomicfile"
	"github.com/jninng/waypoint/internal/safename"
	"github.com/jninng/waypoint/outbox"
)

const (
	dirPerm   os.FileMode = 0o755
	filePerm  os.FileMode = 0o644
	msgSuffix             = ".msg.json"
	tmpSuffix             = ".msg.json.tmp"
)

// FileStore 把 outbox 消息持久化为目录下的 <msgID>.msg.json 文件。
type FileStore struct {
	dir string
}

// New 创建文件后端：MkdirAll 0755，并清理崩溃残留的 *.msg.json.tmp。
func New(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("outbox/filestore: create dir %s: %w", dir, err)
	}
	if err := atomicfile.CleanResidue(dir, tmpSuffix); err != nil {
		return nil, fmt.Errorf("outbox/filestore: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

// Append 实现 outbox.Store：原子追加；ID 重复返回 outbox.ErrDuplicateID。
// 注意：重复检测是 stat-then-write，同一 ID 的并发 Append 不受互斥保护
// （属调用方违约场景；内存后端在同场景下返回 ErrDuplicateID，二者在
// 该违约场景下的行为不同）。
func (s *FileStore) Append(ctx context.Context, msg outbox.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safename.Validate(msg.ID); err != nil {
		return fmt.Errorf("outbox/filestore: append %s: %w", msg.ID, err)
	}
	msg.Normalize()
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox/filestore: append %s: %w", msg.ID, err)
	}
	if _, statErr := os.Stat(s.path(msg.ID)); statErr == nil {
		return fmt.Errorf("outbox/filestore: append %s: %w", msg.ID, outbox.ErrDuplicateID)
	} else if !os.IsNotExist(statErr) {
		return fmt.Errorf("outbox/filestore: append %s: %w", msg.ID, statErr)
	}
	if err := atomicfile.Write(s.path(msg.ID), data, filePerm); err != nil {
		return fmt.Errorf("outbox/filestore: append %s: %w", msg.ID, err)
	}
	return nil
}

// FetchPending 实现 outbox.Store：pending 与 processing 消息，
// CreatedAt 升序 FIFO，值为拷贝。
func (s *FileStore) FetchPending(ctx context.Context) ([]outbox.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("outbox/filestore: read dir %s: %w", s.dir, err)
	}
	var msgs []outbox.Message
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), msgSuffix) {
			continue
		}
		m, err := s.load(trimSuffix(e.Name()))
		if err != nil {
			return nil, err
		}
		if m == nil {
			continue
		}
		if m.Status == outbox.StatusPending || m.Status == outbox.StatusProcessing {
			msgs = append(msgs, *m)
		}
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		if !msgs[i].CreatedAt.Equal(msgs[j].CreatedAt) {
			return msgs[i].CreatedAt.Before(msgs[j].CreatedAt)
		}
		return msgs[i].ID < msgs[j].ID
	})
	return msgs, nil
}

// MarkProcessed 实现 outbox.Store（幂等：消息不存在视为成功）。
func (s *FileStore) MarkProcessed(ctx context.Context, id string) error {
	return s.mutate(ctx, id, func(m *outbox.Message) {
		m.Status = outbox.StatusCompleted
		now := time.Now().UTC()
		m.DeliveredAt = &now
	})
}

// MarkFailed 实现 outbox.Store（幂等：消息不存在视为成功）。
func (s *FileStore) MarkFailed(ctx context.Context, id string, lastError string) error {
	return s.mutate(ctx, id, func(m *outbox.Message) {
		m.Status = outbox.StatusFailed
		m.LastError = lastError
	})
}

// UpdateStatus 实现 outbox.Store（消息不存在返回错误）。
func (s *FileStore) UpdateStatus(ctx context.Context, id string, status outbox.MessageStatus, retries int, lastErr string) error {
	found := false
	err := s.mutate(ctx, id, func(m *outbox.Message) {
		found = true
		m.Status = status
		m.Retries = retries
		m.LastError = lastErr
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("outbox/filestore: update status: message %s not found", id)
	}
	return nil
}

func (s *FileStore) mutate(ctx context.Context, id string, fn func(*outbox.Message)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := safename.Validate(id); err != nil {
		return fmt.Errorf("outbox/filestore: update %s: %w", id, err)
	}
	m, err := s.load(id)
	if err != nil {
		return err
	}
	if m == nil {
		return nil // 幂等：消息不存在视为成功（UpdateStatus 由 found 区分）
	}
	fn(m)
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("outbox/filestore: update %s: %w", id, err)
	}
	if err := atomicfile.Write(s.path(id), data, filePerm); err != nil {
		return fmt.Errorf("outbox/filestore: update %s: %w", id, err)
	}
	return nil
}

// load 读取消息；不存在返回 (nil, nil)。
func (s *FileStore) load(id string) (*outbox.Message, error) {
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("outbox/filestore: load %s: %w", id, err)
	}
	var m outbox.Message
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("outbox/filestore: decode %s: %w", id, err)
	}
	return &m, nil
}

func (s *FileStore) path(id string) string { return filepath.Join(s.dir, id+msgSuffix) }

// trimSuffix 去掉 .msg.json 后缀得到消息 ID。
func trimSuffix(name string) string {
	return strings.TrimSuffix(name, msgSuffix)
}

var _ outbox.Store = (*FileStore)(nil)
