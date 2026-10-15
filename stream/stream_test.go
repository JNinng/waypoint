package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/jninng/waypoint/engine"
	"github.com/jninng/waypoint/state"
)

// memRepo 与 engine 包测试同构（各测试包自持，不进生产导出面）。
type memRepo struct {
	mu     sync.Mutex
	m      map[string][]byte
	saves  int
	failOn int // 第 N 次 Save 失败，模拟「Write 后 Save 前」崩溃窗口
}

func newMemRepo() *memRepo { return &memRepo{m: map[string][]byte{}} }

func (r *memRepo) Load(ctx context.Context, taskID string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.m[taskID]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), data...), nil
}

func (r *memRepo) Save(ctx context.Context, taskID string, s []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saves++
	if r.failOn != 0 && r.saves == r.failOn {
		return errors.New("simulated crash before checkpoint")
	}
	r.m[taskID] = append([]byte(nil), s...)
	return nil
}

func (r *memRepo) Delete(ctx context.Context, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, taskID)
	return nil
}

func (r *memRepo) loadState(t *testing.T, taskID string) *state.BaseTaskState {
	t.Helper()
	data, err := r.Load(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		return nil
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	return &s
}

// sliceSource 按批吐出预置条目；支持记录 Next 的 batchSize。
type sliceSource struct {
	items     [][]byte
	batchSize int
	restored  int
}

func (s *sliceSource) Next(ctx context.Context, batchSize int) ([][]byte, error) {
	s.batchSize = batchSize
	if len(s.items) == 0 {
		return nil, io.EOF
	}
	n := min(batchSize, len(s.items))
	batch := s.items[:n]
	s.items = s.items[n:]
	return batch, nil
}

// recordingSink 收集批次并记录写入批次序列。
type recordingSink struct {
	mu      sync.Mutex
	batches [][][]byte
}

func (s *recordingSink) Write(ctx context.Context, items [][]byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([][]byte, len(items))
	for i, it := range items {
		cp[i] = append([]byte(nil), it...)
	}
	s.batches = append(s.batches, cp)
	return nil
}

func (s *recordingSink) lines() []string {
	var out []string
	for _, b := range s.batches {
		for _, it := range b {
			out = append(out, string(it))
		}
	}
	return out
}

func runPipeline(t *testing.T, repo *memRepo, p *Pipeline[[]byte], taskID string, opts ...engine.RunOption) (*state.BaseTaskState, error) {
	t.Helper()
	return engine.Run(context.Background(), repo, p.AsEngine("test"), taskID, opts...)
}

func TestPipelineRunsToCompletion(t *testing.T) {
	repo := newMemRepo()
	src := &sliceSource{items: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}}
	sink := &recordingSink{}
	p := New[[]byte](src, sink, WithBatchSize[[]byte](2))

	final, err := runPipeline(t, repo, p, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	// 无 Positioner：LSN 回退累计项数。
	if final.CheckpointLSN != 5 {
		t.Fatalf("lsn = %d, want 5 (count)", final.CheckpointLSN)
	}
	got := strings.Join(sink.lines(), ",")
	if got != "a,b,c,d,e" {
		t.Fatalf("sink 收到 %q", got)
	}
	// Payload 记录 count。
	var ns pipelineNS
	if err := json.Unmarshal(extractNS(t, final.Payload), &ns); err != nil {
		t.Fatal(err)
	}
	if ns.Count != 5 {
		t.Fatalf("pipeline.count = %d", ns.Count)
	}
	if src.batchSize != 2 {
		t.Fatalf("Next 收到 batchSize = %d", src.batchSize)
	}
}

func extractNS(t *testing.T, payload json.RawMessage) json.RawMessage {
	t.Helper()
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil {
		t.Fatal(err)
	}
	raw, ok := top["pipeline"]
	if !ok {
		t.Fatal("payload 缺少 pipeline 命名空间")
	}
	return raw
}

// 最后一批允许与 io.EOF 同次返回：先处理本批再终止。
func TestPipelineLastBatchWithEOF(t *testing.T) {
	repo := newMemRepo()
	src := &eofWithItemsSource{items: [][]byte{[]byte("x"), []byte("y"), []byte("z")}}
	sink := &recordingSink{}
	final, err := runPipeline(t, repo, New[[]byte](src, sink, WithBatchSize[[]byte](10)), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	if final.CheckpointLSN != 3 {
		t.Fatalf("最后一批必须被处理并计数, lsn = %d", final.CheckpointLSN)
	}
	if strings.Join(sink.lines(), ",") != "x,y,z" {
		t.Fatalf("sink = %v", sink.lines())
	}
}

type eofWithItemsSource struct {
	items [][]byte
	done  bool
}

func (s *eofWithItemsSource) Next(ctx context.Context, batchSize int) ([][]byte, error) {
	if s.done {
		return nil, io.EOF
	}
	s.done = true
	return s.items, io.EOF
}

// 空任务（源立刻耗尽、无 Finalizer）也应 completed。
func TestPipelineEmptySource(t *testing.T) {
	repo := newMemRepo()
	src := &sliceSource{}
	sink := &recordingSink{}
	final, err := runPipeline(t, repo, New[[]byte](src, sink), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted || final.CheckpointLSN != 0 {
		t.Fatalf("final = %+v", final)
	}
}

// 约束 §8-14：其他命名空间在管道写入时保留。
func TestPipelinePreservesForeignNamespaces(t *testing.T) {
	repo := newMemRepo()
	// 预置带外部命名空间的状态：直接跳过 pending，从 running 起步。
	seed := state.BaseTaskState{
		TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning,
		Payload: json.RawMessage(`{"custom":{"keep":true}}`),
	}
	data, _ := json.Marshal(&seed)
	repo.m["t1"] = data

	src := &sliceSource{items: [][]byte{[]byte("a")}}
	sink := &recordingSink{}
	final, err := runPipeline(t, repo, New[[]byte](src, sink), "t1")
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(final.Payload, &top); err != nil {
		t.Fatal(err)
	}
	if string(top["custom"]) != `{"keep":true}` {
		t.Fatalf("外部命名空间被破坏: %s", top["custom"])
	}
	if _, ok := top["pipeline"]; !ok {
		t.Fatal("pipeline 命名空间未写入")
	}
}

// 非法 Payload 必须报错，不得静默当空处理（约束 §8-14）。
func TestPipelineInvalidPayloadErrors(t *testing.T) {
	repo := newMemRepo()
	// 外层状态 JSON 合法、但 Payload 不是命名空间对象（数组）——
	// 到达管道层解析时报错。
	data, err := json.Marshal(&state.BaseTaskState{
		TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning,
		Payload: json.RawMessage(`[1,2,3]`),
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.m["t1"] = data

	src := &sliceSource{items: [][]byte{[]byte("a")}}
	sink := &recordingSink{}
	_, rerr := runPipeline(t, repo, New[[]byte](src, sink), "t1")
	if rerr == nil || !strings.Contains(rerr.Error(), "parse task payload") {
		t.Fatalf("非法 Payload 必须报错: %v", rerr)
	}
}

// positionSink / positionSource 用于配对规则测试。
type positionSink struct {
	recordingSink
	pos int64
}

func (s *positionSink) Position() int64 { return s.pos }
func (s *positionSink) Align(ctx context.Context, target state.LSN) error {
	s.pos = target
	return nil
}

type positionSource struct {
	sliceSource
	pos int64
}

func (s *positionSource) Position() int64 { return s.pos }
func (s *positionSource) Align(ctx context.Context, target state.LSN) error {
	s.pos = target
	return nil
}

func TestPipelineLSNUsesSinkPositioner(t *testing.T) {
	src := &sliceSource{items: [][]byte{[]byte("a"), []byte("b")}}
	sink := &positionSink{}
	sink.pos = 500 // 模拟已提交物理边界（如输出文件字节）
	p := New[[]byte](src, sink)
	// 跳过第一步 pending→running，直接观察业务步。
	eng := p.AsEngine("test")
	s := &state.BaseTaskState{TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning}
	lsn, err := eng.Execute(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 500 {
		t.Fatalf("lsn = %d, want sink position 500", lsn)
	}
}

func TestPipelineLSNUsesSourcePositioner(t *testing.T) {
	src := &positionSource{}
	src.items = [][]byte{[]byte("a")}
	src.pos = 42
	sink := &recordingSink{}
	p := New[[]byte](src, sink)
	eng := p.AsEngine("test")
	s := &state.BaseTaskState{TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning}
	lsn, err := eng.Execute(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 42 {
		t.Fatalf("lsn = %d, want source position 42", lsn)
	}
}

func TestPipelinePairingViolations(t *testing.T) {
	// 两侧都实现 Positioner → 拒绝。
	src := &positionSource{sliceSource: sliceSource{items: [][]byte{[]byte("a")}}}
	sink := &positionSink{}
	p := New[[]byte](src, sink)
	s := &state.BaseTaskState{TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning}
	_, err := p.Execute(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "both implement Positioner") {
		t.Fatalf("双侧 Positioner 必须拒绝: %v", err)
	}
	// Align 路径同样校验。
	err = p.AsEngine("test").(engine.Aligner).Align(context.Background(), 0)
	if err == nil {
		t.Fatal("Align 路径也必须校验配对")
	}

	// Positioner 无 Aligner → 拒绝。
	src2 := &posNoAlign{sliceSource: sliceSource{items: [][]byte{[]byte("a")}}}
	p2 := New[[]byte](src2, &recordingSink{})
	_, err = p2.Execute(context.Background(), &state.BaseTaskState{Phase: state.PhaseRunning})
	if err == nil || !strings.Contains(err.Error(), "engine.Aligner") {
		t.Fatalf("Positioner 必须成对实现 Aligner: %v", err)
	}
}

type posNoAlign struct{ sliceSource }

func (s *posNoAlign) Position() int64 { return 0 }

func TestAsEngineOptionalInterfaces(t *testing.T) {
	// 无 Positioner / Aligner / Sizer：基础引擎，无可选接口。
	src := &sliceSource{}
	sink := &recordingSink{}
	base := New[[]byte](src, sink).AsEngine("test")
	if _, ok := base.(engine.Aligner); ok {
		t.Fatal("无 Aligner 能力不得实现 Aligner")
	}
	if _, ok := base.(engine.Progressor); ok {
		t.Fatal("无 Sizer 能力不得实现 Progressor")
	}

	// 源侧 Positioner+Aligner（无 Sizer）：只实现 Aligner。
	src2 := &positionSource{}
	eng2 := New[[]byte](src2, &recordingSink{}).AsEngine("test")
	if _, ok := eng2.(engine.Aligner); !ok {
		t.Fatal("源侧 Aligner 未透出")
	}
	if _, ok := eng2.(engine.Progressor); ok {
		t.Fatal("无 Sizer 不得实现 Progressor")
	}

	// Positioner + Sizer：实现 Progressor，进度按 position/size 估算。
	src3 := &sizedSource{positionSource: positionSource{sliceSource: sliceSource{items: [][]byte{[]byte("a")}}, pos: 50}, size: 200}
	eng3 := New[[]byte](src3, &recordingSink{}).AsEngine("test")
	pr, ok := eng3.(engine.Progressor)
	if !ok {
		t.Fatal("Positioner+Sizer 应实现 Progressor")
	}
	s := state.BaseTaskState{}
	if got := pr.Progress(s); got != 25 {
		t.Fatalf("progress = %d, want 25", got)
	}
	// align+progress 同时具备：两个可选接口都实现。
	eng3b, ok := eng3.(engine.Aligner)
	if !ok {
		t.Fatal("Aligner 未透出")
	}
	_ = eng3b
}

type sizedSource struct {
	positionSource
	size int64
}

func (s *sizedSource) Size() int64 { return s.size }

// 组件 Stater 全量还原：重试回滚 Payload 后组件状态随之回退。
func TestStaterRestoreOnRetry(t *testing.T) {
	repo := newMemRepo()
	src := &statedSource{items: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")}}
	sink := &statedSink{} // 第一次 Write 失败（瞬态）
	p := New[[]byte](src, sink, WithBatchSize[[]byte](2))

	final, err := engine.Run(context.Background(), repo, p.AsEngine("test"), "t1", engine.WithRetry(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
	// 关键断言：失败批被重放，但未成功批不得重复写入。
	if got := strings.Join(sink.lines(), ","); got != "a,b,c,d" {
		t.Fatalf("重放语义破坏: got %q", got)
	}
	// Stater 状态经历：还原(空)→写入→还原(count=2)→(失败)→还原(count=2)→写入→…
	if len(sink.restoredCounts) < 3 {
		t.Fatalf("每次 Execute 都必须全量还原: %v", sink.restoredCounts)
	}
}

// statedSource 记录还原游标。
type statedSource struct {
	items    [][]byte
	cursor   int
	restores []int
}

func (s *statedSource) Next(ctx context.Context, batchSize int) ([][]byte, error) {
	if s.cursor >= len(s.items) {
		return nil, io.EOF
	}
	end := min(s.cursor+batchSize, len(s.items))
	batch := s.items[s.cursor:end]
	s.cursor = end
	return batch, nil
}

func (s *statedSource) MarshalState() (json.RawMessage, error) {
	return json.Marshal(map[string]int{"cursor": s.cursor})
}

func (s *statedSource) UnmarshalState(data json.RawMessage) error {
	if len(data) == 0 {
		s.cursor = 0
		s.restores = append(s.restores, 0)
		return nil
	}
	var st struct {
		Cursor int `json:"cursor"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	s.cursor = st.Cursor
	s.restores = append(s.restores, st.Cursor)
	return nil
}

// statedSink 首次 Write 返回错误（瞬态故障）；记录每次还原的 count。
type statedSink struct {
	recordingSink
	failed         bool
	writes         int
	restoredCounts []int
}

func (s *statedSink) Write(ctx context.Context, items [][]byte) error {
	s.writes++
	if !s.failed {
		s.failed = true
		return fmt.Errorf("write %d failed", s.writes)
	}
	return s.recordingSink.Write(ctx, items)
}

func (s *statedSink) MarshalState() (json.RawMessage, error) {
	return json.Marshal(map[string]int{"w": s.writes})
}

func (s *statedSink) UnmarshalState(data json.RawMessage) error {
	if len(data) == 0 {
		s.writes = 0
		s.restoredCounts = append(s.restoredCounts, 0)
		return nil
	}
	var st struct {
		W int `json:"w"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	s.writes = st.W
	s.restoredCounts = append(s.restoredCounts, st.W)
	return nil
}

// 回归：从磁盘恢复的 payload 为 null（首步 pending→running 保存后崩溃）
// 时，后续 persist 不得 panic，且行为等价全新状态。
func TestPipelineNullPayloadRestoredFromDisk(t *testing.T) {
	repo := newMemRepo()
	// 直接持久化 running + payload null（等价于首步保存后崩溃再 Load）。
	data, err := json.Marshal(&state.BaseTaskState{
		TaskID: "t1", TaskType: "test", Phase: state.PhaseRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	repo.m["t1"] = data
	// 重新 Load 模拟恢复（Unmarshal 后 Payload == "null"）。
	loaded, err := repo.Load(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	var s state.BaseTaskState
	if err := json.Unmarshal(loaded, &s); err != nil {
		t.Fatal(err)
	}
	src := &sliceSource{items: [][]byte{[]byte("a")}}
	sink := &recordingSink{}
	p := New[[]byte](src, sink)
	if _, err := p.Execute(context.Background(), &s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(context.Background(), &s); err != nil {
		t.Fatal(err)
	}
	if s.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", s.Phase)
	}
	if strings.Join(sink.lines(), ",") != "a" {
		t.Fatalf("sink = %v", sink.lines())
	}
}

// Finalizer 在源耗尽时调用。
type finalizingSink struct {
	recordingSink
	finalized bool
}

func TestPipelineCallsFinalizerOnEOF(t *testing.T) {
	repo := newMemRepo()
	src := &sliceSource{items: [][]byte{[]byte("a"), []byte("b")}}
	sink := &finalizingSink{recordingSink: recordingSink{}}
	// finalizingSink 内嵌 recordingSink 并实现 Finalizer。
	p := New[[]byte](src, sink)
	final, err := engine.Run(context.Background(), repo, p.AsEngine("test"), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !sink.finalized {
		t.Fatal("源耗尽后必须调用 Finalize")
	}
	if final.Phase != state.PhaseCompleted {
		t.Fatalf("phase = %q", final.Phase)
	}
}

func (s *finalizingSink) Finalize(ctx context.Context) error {
	s.finalized = true
	return nil
}
