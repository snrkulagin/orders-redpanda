// Package order implements order creation: it builds the Order aggregate and
// publishes it on the orders.created topic.
package order

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	orderdomain "github.com/snrkulagin/orders-redpanda/internal/domain/order"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
)

// EventPublisher is the subset of the Kafka client the service depends on.
// A *kafka.Producer satisfies it in production, a fake in tests.
type EventPublisher interface {
	Publish(ctx context.Context, topic string, key, value []byte) error
}

// Service creates orders and emits the corresponding orders.created event.
type Service struct {
	publisher EventPublisher
}

// NewService wires a Service to the given event publisher.
func NewService(publisher EventPublisher) *Service {
	return &Service{publisher: publisher}
}

// CreateOrderRequest is the payload accepted by POST /orders.
type CreateOrderRequest struct {
	UserID int64              `json:"user_id"`
	Items  []orderdomain.Item `json:"items"`
}

// CreateOrder validates the request, builds an Order with a fresh ID and
// publishes it on kafka.TopicOrdersCreated.
func (s *Service) CreateOrder(ctx context.Context, req CreateOrderRequest) (orderdomain.Order, error) {
	if req.UserID == 0 {
		return orderdomain.Order{}, errors.New("user_id is required")
	}
	if len(req.Items) == 0 {
		return orderdomain.Order{}, errors.New("items must not be empty")
	}

	var amount float64
	for _, item := range req.Items {
		if item.Quantity <= 0 {
			return orderdomain.Order{}, fmt.Errorf("item %q: quantity must be positive", item.ProductID)
		}
		amount += item.Price * float64(item.Quantity)
	}

	o := orderdomain.Order{
		OrderID:   uuid.NewString(),
		UserID:    req.UserID,
		Items:     req.Items,
		Amount:    amount,
		CreatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(o)
	if err != nil {
		return orderdomain.Order{}, fmt.Errorf("marshal order: %w", err)
	}

	if err := s.publisher.Publish(ctx, kafka.TopicOrdersCreated, []byte(o.OrderID), payload); err != nil {
		return orderdomain.Order{}, fmt.Errorf("publish %s: %w", kafka.TopicOrdersCreated, err)
	}

	return o, nil
}
