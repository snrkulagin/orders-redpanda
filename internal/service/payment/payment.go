package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	paymentdomain "github.com/snrkulagin/orders-redpanda/internal/domain/payment"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
)

// paidChance is the percentage of processed payments that succeed; the
// rest fail. Trying to simulate an actual payment processor's output.
const paidChance = 90

// ErrAlreadyProcessed is returned by ProcessPayment when orderID has
// already been paid/failed before — most commonly a redelivered
// orders.created record after a consumer-group rebalance (Kafka is only
// at-least-once). Callers should treat it as "skip", not as a failure.
var ErrAlreadyProcessed = errors.New("payment: order already processed")

type EventPublisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

// Dedup records that an order has been processed and reports whether it
// was seen for the first time, atomically.
type Dedup interface {
	MarkProcessed(ctx context.Context, orderID string) (firstTime bool, err error)
}

// Service simulates payment processing and emits its outcome.
type Service struct {
	publisher EventPublisher
	dedup     Dedup
}

// NewService wires a Service to the given event publisher and dedup store.
func NewService(publisher EventPublisher, dedup Dedup) *Service {
	return &Service{publisher: publisher, dedup: dedup}
}

// ProcessPayment randomly decides whether orderID's payment succeeds
// (paidChance% of the time) or fails, and publishes the outcome on
// kafka.TopicOrdersPaymentProcessed. If orderID has already been
// processed before, it does neither and returns ErrAlreadyProcessed —
// the dedup check runs first so a redelivered record can't roll the dice
// twice and publish two different outcomes for the same order.
func (s *Service) ProcessPayment(ctx context.Context, orderID string) (paymentdomain.Payment, error) {
	firstTime, err := s.dedup.MarkProcessed(ctx, orderID)
	if err != nil {
		return paymentdomain.Payment{}, fmt.Errorf("payment: dedup check for order_id=%s: %w", orderID, err)
	}
	if !firstTime {
		return paymentdomain.Payment{}, ErrAlreadyProcessed
	}

	status := paymentdomain.StatusFailed
	if rand.IntN(100) < paidChance {
		status = paymentdomain.StatusPaid
	}

	p := paymentdomain.Payment{
		OrderID:     orderID,
		Status:      status,
		ProcessedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(p)
	if err != nil {
		return paymentdomain.Payment{}, fmt.Errorf("marshal payment: %w", err)
	}

	if err := s.publisher.Publish(ctx, kafka.TopicOrdersPaymentProcessed, []byte(orderID), payload); err != nil {
		return paymentdomain.Payment{}, fmt.Errorf("publish %s: %w", kafka.TopicOrdersPaymentProcessed, err)
	}

	return p, nil
}
