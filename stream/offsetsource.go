package stream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/jninng/waypoint/state"
)

// OffsetSource 按字节偏移逐行读取文本文件（JSONL 等），断点续读。
//
// 实现 Source[[]byte]（返回不含行尾符的原始行）、Stater（字节偏移）、
// Positioner（偏移）、engine.Aligner（源侧补偿）。
//
// 字节偏移按包含行分隔符的原始字节数累计——读取使用「返回原始字节
// （含分隔符）」的按行读取（bufio.Reader.ReadBytes('\n')），不使用会
// 剥离行尾符/\r 的行扫描器计算断点偏移。兼容 \n、\r\n、末行无换行；
// 不支持裸 \r 行尾（经典 Mac 风格）。
//
// 管道 LSN = Source.Position()（源是补偿侧）；批量行解码失败由适配层的
// Sink/调用方处理。
//
// 并发边界：单 goroutine 使用（在 Run 的 goroutine 内同步执行）。
type OffsetSource struct {
	path   string
	offset int64
}

// NewOffsetSource 创建偏移行源。
func NewOffsetSource(path string) *OffsetSource {
	return &OffsetSource{path: path}
}

type offsetState struct {
	Offset int64 `json:"offset"`
}

// Next 实现 Source[[]byte]：读取至多 batchSize 行。
// 最后一批允许与 io.EOF 同次返回；彻底耗尽后返回 (nil, io.EOF)。
// 每批打开并关闭源文件，不持有跨批句柄。
func (s *OffsetSource) Next(ctx context.Context, batchSize int) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("stream: open source %s: %w", s.path, err)
	}
	defer f.Close()
	if s.offset > 0 {
		if _, err := f.Seek(s.offset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("stream: seek source %s to %d: %w", s.path, s.offset, err)
		}
	}
	r := bufio.NewReader(f)
	var items [][]byte
	for len(items) < batchSize {
		raw, rerr := r.ReadBytes('\n')
		if len(raw) > 0 {
			s.offset += int64(len(raw)) // 按含分隔符的原始字节数累计
			items = append(items, trimEOL(raw))
		}
		if rerr == io.EOF {
			if len(items) > 0 {
				return items, io.EOF
			}
			return nil, io.EOF
		}
		if rerr != nil {
			return nil, fmt.Errorf("stream: read source %s: %w", s.path, rerr)
		}
	}
	return items, nil
}

// trimEOL 剥离行尾的 \n 与紧随其前的 \r（\r\n / \n / 末行无换行）。
func trimEOL(raw []byte) []byte {
	line := raw
	if bytes.HasSuffix(line, []byte{'\n'}) {
		line = line[:len(line)-1]
	}
	if bytes.HasSuffix(line, []byte{'\r'}) {
		line = line[:len(line)-1]
	}
	return line
}

// Position 实现 Positioner：已提交物理边界（字节偏移）。
func (s *OffsetSource) Position() int64 { return s.offset }

// MarshalState 实现 Stater。
func (s *OffsetSource) MarshalState() (json.RawMessage, error) {
	raw, err := json.Marshal(offsetState{Offset: s.offset})
	if err != nil {
		return nil, fmt.Errorf("stream: marshal offset state: %w", err)
	}
	return raw, nil
}

// UnmarshalState 实现 Stater：全量还原（空数据 = 回退初始状态）。
func (s *OffsetSource) UnmarshalState(data json.RawMessage) error {
	if emptyState(data) {
		s.offset = 0
		return nil
	}
	var st offsetState
	if err := json.Unmarshal(data, &st); err != nil {
		return fmt.Errorf("stream: parse offset state: %w", err)
	}
	if st.Offset < 0 {
		return fmt.Errorf("stream: invalid offset %d", st.Offset)
	}
	s.offset = st.Offset
	return nil
}

// Align 实现 engine.Aligner（源侧补偿）：校验源文件大小 ≥ targetLSN，
// 小于则返回错误拒绝恢复（源文件可能被截断/替换）。
func (s *OffsetSource) Align(ctx context.Context, targetLSN state.LSN) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(s.path)
	if err != nil {
		return fmt.Errorf("stream: stat source %s: %w", s.path, err)
	}
	if info.Size() < targetLSN {
		return fmt.Errorf("stream: source %s size %d < checkpoint lsn %d (source truncated or replaced?)", s.path, info.Size(), targetLSN)
	}
	s.offset = targetLSN
	return nil
}
