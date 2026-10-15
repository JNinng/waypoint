// 示例：saga 模式——多步补偿事务（设计 §6.4 / §5.7、ADR-4/ADR-9）。
//
// 场景一：扣款步骤重试耗尽 → 逆序补偿已完成步骤（先冲正扣款、再释放
// 库存），终态 compensated（已回滚）；原始失败原因持久保留于 StepErrors。
// 场景二：补偿中途被取消（模拟进程崩溃）→ 状态停于 compensating；
// 篡改步骤定义后 Resume 因指纹不一致被拒绝（ADR-4）；用原定义 Resume
// 续跑，跳过已补偿步骤、重入未完成的补偿，最终 compensated。
//
// 运行：go run ./saga
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jninng/waypoint/saga"
)

func main() {
	store := saga.NewInMemorySagaStore()
	ctx := context.Background()

	fmt.Println("场景一：正向失败 → 逆序补偿 → compensated（已回滚）")
	chargeAttempts := 0
	order := &saga.Saga{
		Name: "order",
		Steps: []saga.SagaStep{
			{
				Name: "reserve-inventory",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					ac["reservation"] = "R-1001"
					fmt.Println("  action reserve-inventory: 预留库存 R-1001")
					return nil
				},
				Compensation: func(ctx context.Context, ac saga.ActionCtx) error {
					fmt.Printf("  compensation reserve-inventory: 释放库存 %v\n", ac["reservation"])
					return nil
				},
			},
			{
				Name: "charge-payment",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					chargeAttempts++
					ac["charge"] = "C-2002"
					fmt.Printf("  action charge-payment: 第 %d 次尝试扣款 → 拒卡\n", chargeAttempts)
					return errors.New("card declined")
				},
				// 注意：本步最终 failed（从未 completed），saga 只补偿
				// 「已完成」的步骤——拒卡意味着资金未变动，下面的退款
				// 补偿不会被调用；被逆序补偿的只有已完成的「预留库存」。
				Compensation: func(ctx context.Context, ac saga.ActionCtx) error {
					fmt.Printf("  compensation charge-payment: 冲正扣款 %v\n", ac["charge"])
					return nil
				},
				MaxRetries: 2,
			},
			{
				Name: "ship",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					fmt.Println("  action ship: 发货")
					return nil
				},
				// Compensation 为 nil：幂等步骤无需补偿。
			},
		},
	}
	st, err := saga.NewSagaCoordinator(store).Run(ctx, order, "order-1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("终态: status=%s stepStatuses=%v（charge 从未完成 → 无需补偿；ship 未开始）\n",
		st.Status, st.StepStatuses)
	for i, e := range st.StepErrors {
		if e != "" {
			fmt.Printf("  失败原因 step[%d] %s: %s\n", i, order.Steps[i].Name, e)
		}
	}

	fmt.Println("场景二：补偿中途取消 → Resume 依据指纹与 StepStatuses 续跑")
	reserveComps, chargeComps := 0, 0
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recovery := &saga.Saga{
		Name: "order",
		Steps: []saga.SagaStep{
			{
				Name: "reserve-inventory",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					ac["reservation"] = "R-3003"
					fmt.Println("  action reserve-inventory: 预留库存 R-3003")
					return nil
				},
				Compensation: func(ctx context.Context, ac saga.ActionCtx) error {
					reserveComps++
					fmt.Printf("  compensation reserve-inventory: 释放库存 %v\n", ac["reservation"])
					return nil
				},
			},
			{
				Name: "charge-payment",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					ac["charge"] = "C-4004"
					fmt.Println("  action charge-payment: 扣款成功 C-4004")
					return nil
				},
				Compensation: func(ctx context.Context, ac saga.ActionCtx) error {
					chargeComps++
					if chargeComps == 1 {
						cancel() // 模拟补偿进行中进程被终止
						<-ctx.Done()
						return ctx.Err()
					}
					fmt.Printf("  compensation charge-payment: 冲正扣款 %v（Resume 重入成功）\n", ac["charge"])
					return nil
				},
			},
			{
				Name: "ship",
				Action: func(ctx context.Context, ac saga.ActionCtx) error {
					return errors.New("address unreachable") // 正向失败触发补偿
				},
			},
		},
	}
	coord := saga.NewSagaCoordinator(store)
	if _, err := coord.Run(cancelCtx, recovery, "order-2"); !errors.Is(err, context.Canceled) {
		log.Fatalf("期望因取消中断: %v", err)
	}
	mid, _ := store.Load(context.Background(), "order-2")
	fmt.Printf("中断后: status=%s stepStatuses=%v\n", mid.Status, mid.StepStatuses)

	// 篡改步骤定义（追加一步）→ 指纹不一致 → 拒绝恢复（ADR-4）。
	tampered := &saga.Saga{Name: "order", Steps: append(append([]saga.SagaStep{}, recovery.Steps...),
		saga.SagaStep{Name: "extra"})}
	if _, err := coord.Resume(context.Background(), tampered, "order-2"); err != nil {
		fmt.Printf("篡改定义后 Resume 被拒绝: %v\n", err)
	}

	final, err := coord.Resume(context.Background(), recovery, "order-2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("恢复完成: status=%s stepStatuses=%v\n", final.Status, final.StepStatuses)
	fmt.Printf("补偿执行计数: reserve=%d（不重复）charge=%d（重入 1 次）\n", reserveComps, chargeComps)
}
