package payment

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	paymentdomain "github.com/snrkulagin/orders-redpanda/internal/domain/payment"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
)

// paidChance is the percentage of processed payments that succeed; the
// rest fail. Trying to simulate an actual payment processor's output.
const paidChance = 90

type EventPublisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

// Service simulates payment processing and emits its outcome.
type Service struct {
	publisher EventPublisher
}

// NewService wires a Service to the given event publisher.
func NewService(publisher EventPublisher) *Service {
	return &Service{publisher: publisher}
}

// ProcessPayment randomly decides whether orderID's payment succeeds
// (paidChance% of the time) or fails, and publishes the outcome on
// kafka.TopicOrdersPaymentProcessed.
func (s *Service) ProcessPayment(ctx context.Context, orderID string) (paymentdomain.Payment, error) {
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
