package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/jninng/waypoint/internal/atomicfile"
)

const (
	defaultBatchesPerChunk             = 10
	defaultFilePerm        fs.FileMode = 0o644
	dirPerm                fs.FileMode = 0o755
)

// ChunkedSink 把批次逐行写入编号分块临时文件，收尾时原子合并为最终文件。
//
// 实现 Sink[[]byte]（每项一行，追加行尾符）、Stater、Finalizer。
// 项为原始行字节（不含行尾符）——行编码（JSON/CSV 等）由 Source 适配层
// 完成，汇只做字节追加，不解释内容。
//
// 文件布局（以 finalName 相对 dir 解析，可含子目录前缀）：
//
//	<finalName>.chunk_<N>.tmp   分块（重放输入，Finalize 后仍保留）
//	<finalName>.final.tmp       合并临时文件
//	<finalName>                 最终产物（原子出现）
//
// 截断重放不变量：Write 打开当前分块时，先截断到 Stater 记录的已提交字节
// 边界，再追加本批——「上次 Execute 已写但未 checkpoint」的多余字节被消除，
// 重放不重复、不乱序。
//
// Stater 状态：{ "chunk_idx": int, "committed_bytes": int64 }（当前块）。
// 分块滚动按批数尽力而为：恢复重放后当前块可能多容纳若干批，不改变合并
// 结果的正确性（分块只是中间产物）。
//
// LSN：分块模式下汇无 Positioner/Aligner（重放由截断保证安全），管道 LSN
// 回退「累计已处理项数」，块进度由 Stater 承载。
//
// 并发边界：单 goroutine 使用（在 Run 的 goroutine 内同步执行）。
type ChunkedSink struct {
	dir      string
	finalRel string // 最终文件名（相对 dir，可含子目录前缀）

	batchesPerChunk int
	lineSuffix      []byte
	perm            fs.FileMode

	chunkIdx       int
	committedBytes int64
	batchesInChunk int
}

// ChunkedSinkOption 配置 ChunkedSink。
type ChunkedSinkOption func(*ChunkedSink)

// WithBatchesPerChunk 设置每块包含的批数（默认 10）；n < 1 时 panic。
func WithBatchesPerChunk(n int) ChunkedSinkOption {
	if n < 1 {
		panic(fmt.Sprintf("stream: WithBatchesPerChunk must be >= 1, got %d", n))
	}
	return func(s *ChunkedSink) { s.batchesPerChunk = n }
}

// WithLineSuffix 设置行尾符（默认 '\n'）；显式传 nil 表示不追加行尾符。
func WithLineSuffix(suffix []byte) ChunkedSinkOption {
	return func(s *ChunkedSink) { s.lineSuffix = suffix }
}

// WithFilePerm 设置分块与最终文件权限（默认 0644）。
func WithFilePerm(perm fs.FileMode) ChunkedSinkOption {
	return func(s *ChunkedSink) { s.perm = perm }
}

// NewChunkedSink 创建分块文件汇；finalName 相对 dir 解析。
func NewChunkedSink(dir, finalName string, opts ...ChunkedSinkOption) *ChunkedSink {
	s := &ChunkedSink{
		dir:             dir,
		finalRel:        finalName,
		batchesPerChunk: defaultBatchesPerChunk,
		lineSuffix:      []byte("\n"),
		perm:            defaultFilePerm,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type chunkedState struct {
	ChunkIdx       int   `json:"chunk_idx"`
	CommittedBytes int64 `json:"committed_bytes"`
}

// MarshalState 实现 Stater。
func (s *ChunkedSink) MarshalState() (json.RawMessage, error) {
	raw, err := json.Marshal(chunkedState{ChunkIdx: s.chunkIdx, CommittedBytes: s.committedBytes})
	if err != nil {
		return nil, fmt.Errorf("stream: marshal chunked sink state: %w", err)
	}
	return raw, nil
}

// UnmarshalState 实现 Stater：全量还原（空数据 = 回退初始状态）。
func (s *ChunkedSink) UnmarshalState(data json.RawMessage) error {
	s.batchesInChunk = 0
	if emptyState(data) {
		s.chunkIdx, s.committedBytes = 0, 0
		return nil
	}
	var st chunkedState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("stream: parse chunked sink state: %w", err)
	}
	if st.ChunkIdx < 0 || st.CommittedBytes < 0 {
		return fmt.Errorf("stream: invalid chunked sink state %+v", st)
	}
	s.chunkIdx, s.committedBytes = st.ChunkIdx, st.CommittedBytes
	return nil
}

// Write 实现 Sink[[]byte]：先截断当前分块到已提交边界，再追加本批。
// 每批打开并关闭分块文件，不持有跨批句柄。
func (s *ChunkedSink) Write(ctx context.Context, items [][]byte) error {
	if len(items) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.fileDir(), dirPerm); err != nil {
		return fmt.Errorf("stream: create chunk dir: %w", err)
	}
	f, err := s.openChunk()
	if err != nil {
		return err
	}
	var buf []byte
	for _, item := range items {
		buf = append(buf, item...)
		buf = append(buf, s.lineSuffix...)
	}
	_, werr := f.Write(buf)
	cerr := f.Close()
	if werr != nil {
		return fmt.Errorf("stream: write chunk %d: %w", s.chunkIdx, werr)
	}
	if cerr != nil {
		return fmt.Errorf("stream: close chunk %d: %w", s.chunkIdx, cerr)
	}
	// 物理追加成功后才推进已提交边界；失败尝试的多余字节由下次截断消除。
	s.committedBytes += int64(len(buf))
	s.batchesInChunk++
	if s.batchesInChunk >= s.batchesPerChunk {
		s.rollChunk()
	}
	return nil
}

// Finalize 实现 Finalizer：按块序拼接全部分块至 <finalName>.final.tmp，
// Sync + Rename 为最终文件——最终产物原子出现；不得删除分块（分块是
// 重放输入）。可重入：重放时重新拼接 + Rename。
func (s *ChunkedSink) Finalize(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(s.fileDir(), dirPerm); err != nil {
		return fmt.Errorf("stream: create final dir: %w", err)
	}
	var (
		readers  []io.Reader
		closers  []*os.File
		expected int64
	)
	defer func() {
		for _, f := range closers {
			f.Close()
		}
	}()
	for i := 0; i <= s.chunkIdx; i++ {
		limit := s.limitFor(i)
		f, err := os.Open(s.chunkPath(i))
		if err != nil {
			if os.IsNotExist(err) && limit == 0 {
				continue // 末块尚未创建：视作空块
			}
			return fmt.Errorf("stream: open chunk %d: %w", i, err)
		}
		closers = append(closers, f)
		if limit < 0 {
			info, serr := f.Stat()
			if serr != nil {
				return fmt.Errorf("stream: stat chunk %d: %w", i, serr)
			}
			expected += info.Size()
			readers = append(readers, f)
		} else {
			expected += limit
			readers = append(readers, io.LimitReader(f, limit))
		}
	}
	written, err := atomicfile.Save(s.finalPath(), s.finalPath()+".final.tmp",
		io.MultiReader(readers...), s.perm)
	if err != nil {
		return fmt.Errorf("stream: finalize %s: %w", s.finalPath(), err)
	}
	if written != expected {
		return fmt.Errorf("stream: finalize %s: copied %d bytes, want %d (chunk shorter than committed boundary)", s.finalPath(), written, expected)
	}
	return nil
}

// Cleanup 删除全部分块与残留 .final.tmp。
//
// 必须在 completed checkpoint Save 成功之后由调用方显式调用（ADR-3）；
// 在终态落盘前调用属调用方违约——分块丢失后 completed 无法重放。
// 返回 error，目录读取与删除失败可见（多错误以 errors.Join 汇总）；
// 最终产物本身不被删除。
func (s *ChunkedSink) Cleanup() error {
	entries, err := os.ReadDir(s.fileDir())
	if err != nil {
		return fmt.Errorf("stream: cleanup read %s: %w", s.fileDir(), err)
	}
	var errs []error
	chunkPrefix := s.baseName() + ".chunk_"
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, chunkPrefix) && strings.HasSuffix(name, ".tmp") ||
			name == s.baseName()+".final.tmp" {
			if rerr := os.Remove(filepath.Join(s.fileDir(), name)); rerr != nil && !os.IsNotExist(rerr) {
				errs = append(errs, fmt.Errorf("stream: cleanup remove %s: %w", name, rerr))
			}
		}
	}
	return errors.Join(errs...)
}

// limitFor 返回拼接第 i 块时的字节上限：末块取已提交边界，其余整块（-1）。
func (s *ChunkedSink) limitFor(i int) int64 {
	if i == s.chunkIdx {
		return s.committedBytes
	}
	return -1
}

// openChunk 打开当前分块并截断到已提交边界，返回可追加的句柄。
func (s *ChunkedSink) openChunk() (*os.File, error) {
	f, err := os.OpenFile(s.chunkPath(s.chunkIdx), os.O_RDWR|os.O_CREATE, s.perm)
	if err != nil {
		return nil, fmt.Errorf("stream: open chunk %d: %w", s.chunkIdx, err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stream: stat chunk %d: %w", s.chunkIdx, err)
	}
	switch {
	case info.Size() > s.committedBytes:
		// 截断重放：消除「上次 Execute 已写但未 checkpoint」的多余字节。
		if err := f.Truncate(s.committedBytes); err != nil {
			f.Close()
			return nil, fmt.Errorf("stream: truncate chunk %d to %d: %w", s.chunkIdx, s.committedBytes, err)
		}
	case info.Size() < s.committedBytes:
		f.Close()
		return nil, fmt.Errorf("stream: chunk %d size %d < committed boundary %d", s.chunkIdx, info.Size(), s.committedBytes)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("stream: seek chunk %d: %w", s.chunkIdx, err)
	}
	return f, nil
}

func (s *ChunkedSink) rollChunk() {
	s.chunkIdx++
	s.committedBytes = 0
	s.batchesInChunk = 0
}

// fileDir 是分块与最终文件共同所在的目录（finalName 可能含子目录前缀）。
func (s *ChunkedSink) fileDir() string { return filepath.Join(s.dir, filepath.Dir(s.finalRel)) }

// baseName 是最终文件基名（分块/合并临时文件的前缀来源）。
func (s *ChunkedSink) baseName() string { return filepath.Base(s.finalRel) }

func (s *ChunkedSink) chunkPath(i int) string {
	return filepath.Join(s.fileDir(), fmt.Sprintf("%s.chunk_%d.tmp", s.baseName(), i))
}

func (s *ChunkedSink) finalPath() string { return filepath.Join(s.dir, s.finalRel) }
