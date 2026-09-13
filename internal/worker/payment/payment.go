// Package paymentworker turns Kafka records into calls into the payment
// service. It plays the same role for the Kafka-consumer-driven
// payment-service that internal/transport/http plays for the HTTP-driven
// order-service: delivery-mechanism glue only — no client/DB setup lives
// here, that's main.go's job. App just takes already-wired dependencies and
// uses them, so main can share a single producer/pg/etc. across as many
// use cases as a worker ends up needing instead of each one standing up its
// own copy.
package paymentworker

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	orderdomain "github.com/snrkulagin/orders-redpanda/internal/domain/order"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
	"github.com/snrkulagin/orders-redpanda/internal/service/payment"
)

// simulatedProcessingDelay slows down each record on purpose — long enough
// to give the consumer-group rebalance/redelivery demo (step 2 of
// kafka-mini-project.md) time to actually catch an instance mid-processing.
const simulatedProcessingDelay = 500 * time.Millisecond

// App consumes orders.created and processes payments through svc. It owns
// none of its dependencies — main.go creates and closes them.
type App struct {
	consumer *kafka.Consumer
	svc      *payment.Service
}

// New wires an already-constructed consumer and payment service into a
// runnable App.
func New(consumer *kafka.Consumer, svc *payment.Service) *App {
	return &App{consumer: consumer, svc: svc}
}

// Run blocks, consuming orders.created until ctx is canceled.
func (a *App) Run(ctx context.Context) error {
	return a.consumer.Consume(ctx, a.handle)
}

func (a *App) handle(ctx context.Context, msg kafka.Message) {
	time.Sleep(simulatedProcessingDelay)

	var o orderdomain.Order
	if err := json.Unmarshal(msg.Value, &o); err != nil {
		log.Printf("payment-service: bad orders.created payload at partition=%d offset=%d: %v",
			msg.Partition, msg.Offset, err)
		return
	}

	result, err := a.svc.ProcessPayment(ctx, o.OrderID)
	if errors.Is(err, payment.ErrAlreadyProcessed) {
		log.Printf("payment-service: order_id=%s already processed, skipping (redelivery)", o.OrderID)
		return
	}
	if err != nil {
		log.Printf("payment-service: process payment for order_id=%s: %v", o.OrderID, err)
		return
	}

	log.Printf("payment-service: order_id=%s status=%s", result.OrderID, result.Status)
}
