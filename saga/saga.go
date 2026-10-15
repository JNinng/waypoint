// Package saga 实现调度层 saga 模式：多步骤事务（每步正向 Action +
// 逆序补偿）的独立状态机，不依赖 engine。
//
// 编排语义（设计 §5.7 / ADR-9）：
//   - 正向按序执行 Action，每步执行前 Save；步骤失败重试耗尽 → 进入
//     compensating，逆序只补偿 StepCompleted 的步骤。
//   - 补偿同样可重试（同一重试链）、可超时（同一 Timeout 施加方式）；
//     单个补偿失败记入 StepErrors 后继续其余补偿（不阻断）。
//   - 终态三分：completed（全部正向成功）/ compensated（正向失败且补偿
//     全部成功，已回滚）/ failed（任一补偿失败，需人工介入）。
//   - 步骤定义指纹：Run 时计算步骤名序列的 SHA-256（名称以 \x00 连接）
//     写入 StepFingerprint；Resume 校验一致才恢复，不一致拒绝
//     （防步骤增删/重排后按旧状态错位补偿或越界，ADR-4）。
//   - ctx 取消：以 context.WithoutCancel 保存当前状态后返回 ctx.Err()；
//     一切 Save 错误检查并上抛，不吞。
//
// 并发边界：单协调器前提——同一 sagaID 同时至多一个协调器实例在推进；
// 并发 Resume 同一 saga 属调用方违约（设计 §2.4）。
package saga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// SagaStatus 是 saga 生命周期状态。
type SagaStatus string

const (
	StatusPending      SagaStatus = "pending"
	StatusRunning      SagaStatus = "running"
	StatusCompensating SagaStatus = "compensating"
	StatusCompleted    SagaStatus = "completed"
	StatusCompensated  SagaStatus = "compensated"
	StatusFailed       SagaStatus = "failed"
)

// StepStatus 是单步状态。
type StepStatus string

const (
	StepPending     StepStatus = "pending"
	StepRunning     StepStatus = "running"
	StepCompleted   StepStatus = "completed"
	StepFailed      StepStatus = "failed"
	StepCompensated StepStatus = "compensated"
)

// ActionCtx 是步骤间共享的数据上下文；值必须为 JSON 可序列化类型，
// 由业务保证（不可序列化值会导致 Save 失败）。
type ActionCtx = map[string]any

// SagaStep 是一步定义：正向 Action + 逆序补偿。
type SagaStep struct {
	Name string
	// Action 正向操作。重入契约：崩溃恢复/Resume 可能重放一次
	// StepRunning 步骤的 Action——实现须可重入（幂等）或能从已提交
	// ActionCtx 检测自身已完成。
	Action func(ctx context.Context, actionCtx ActionCtx) error
	// Compensation 逆序补偿；nil = 幂等步骤（无需补偿，直接标记
	// compensated）。补偿重入依据 StepStatuses 跳过已 compensated 的步骤。
	Compensation func(ctx context.Context, actionCtx ActionCtx) error
	// MaxRetries 最大尝试次数（含首次）；0 = 使用 Saga.DefaultMaxRetries；
	// 解析下限 1。
	MaxRetries int
	// Timeout 单次尝试的超时；0 = 无超时；以 context.WithTimeout 施加于
	// Action 与 Compensation。
	Timeout time.Duration
}

// SagaState 是 saga 执行状态（全部持久化于 SagaStore）。
type SagaState struct {
	SagaID          string       `json:"saga_id"`
	SagaName        string       `json:"saga_name"`
	Status          SagaStatus   `json:"status"`
	CurrentStep     int          `json:"current_step"` // running 期恒等于第一个未完成步骤的索引；compensating 期为当前补偿目标，全部补偿完成后为 -1（无未完成步骤）
	StepStatuses    []StepStatus `json:"step_statuses"`
	StepErrors      []string     `json:"step_errors,omitempty"` // 原始失败原因与补偿失败原因都持久保留（按步骤索引对齐）
	StepFingerprint string       `json:"step_fingerprint"`
	ActionCtx       ActionCtx    `json:"action_ctx"`
	CreatedAt       time.Time    `json:"created_at"`
	UpdatedAt       time.Time    `json:"updated_at"`
}

// SagaStore 是 saga 状态的持久化抽象。
// Load 不存在返回 (nil, nil)；Save 完整替换；Delete 幂等。
type SagaStore interface {
	Load(ctx context.Context, sagaID string) (*SagaState, error)
	Save(ctx context.Context, state *SagaState) error
	Delete(ctx context.Context, sagaID string) error
}

// Saga 是定义：步骤序列与默认配置，不含执行状态。
type Saga struct {
	Name  string
	Steps []SagaStep
	// DefaultMaxRetries 是步骤 MaxRetries=0 时的默认值；解析下限 1。
	DefaultMaxRetries int
}

// stepFingerprint 计算步骤名序列的稳定哈希（SHA-256，名称以 \x00 连接）。
func stepFingerprint(saga *Saga) string {
	h := sha256.New()
	for i, step := range saga.Steps {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(step.Name))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// resolveAttempts 解析步骤最大尝试次数：step → saga 默认 → 1，下限 1。
func resolveAttempts(step SagaStep, saga *Saga) int {
	attempts := step.MaxRetries
	if attempts == 0 {
		attempts = saga.DefaultMaxRetries
	}
	if attempts < 1 {
		attempts = 1
	}
	return attempts
}

// recordStepError 按索引记录错误原因（切片惰性增长，保持与步骤对齐）。
func recordStepError(st *SagaState, i int, msg string) {
	for len(st.StepErrors) <= i {
		st.StepErrors = append(st.StepErrors, "")
	}
	st.StepErrors[i] = msg
}
