package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/transportnats/tnats"
)

type PaymentTask struct {
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	durableBinding := tnats.DurableWork(
		"PAYMENTS_STREAM",
		"payments.process",
		"payment-processor-consumer",
		"payments.dlq",
	).WithDeliveryPolicy(3, 10*time.Second, time.Second, 16)

	paymentAction := action.New(
		"payments.durable.process",
		func(ctx context.Context, task PaymentTask) (string, error) {
			fmt.Printf("💳 Processing payment of $%.2f for %s\n", task.Amount, task.AccountID)

			return "PROCESSED", nil
		},
	).Route(durableBinding).Build()

	transport := tnats.New("nats://127.0.0.1:4222")
	transport.Mount([]action.AnyAction{paymentAction})

	go func() {
		if _, err := transport.Do(ctx, nil); err != nil {
			log.Printf("JetStream transport error: %v", err)
		}
	}()

	fmt.Println("⚡ JetStream Durable Worker running. Press Ctrl+C to stop.")
	<-ctx.Done()
}
