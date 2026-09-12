// Package http exposes order-service's HTTP API.
package http

import (
	"context"
	"net/http"

	"github.com/snrkulagin/orders-redpanda/internal/domain/order"
	orderservice "github.com/snrkulagin/orders-redpanda/internal/service/order"
)

// OrderCreator is the subset of order.Service the HTTP layer depends on.
type OrderCreator interface {
	CreateOrder(ctx context.Context, req orderservice.CreateOrderRequest) (order.Order, error)
}

// NewRouter builds the HTTP router for order-service. Adding an endpoint
// means one more line here plus a small adapter like createOrder below —
// the decode/call/encode plumbing lives once, in handle.
func NewRouter(svc OrderCreator) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("POST /orders", handle(http.StatusCreated, createOrder(svc)))
	return mux
}

func createOrder(svc OrderCreator) func(context.Context, orderservice.CreateOrderRequest) (order.Order, error) {
	return func(ctx context.Context, req orderservice.CreateOrderRequest) (order.Order, error) {
		created, err := svc.CreateOrder(ctx, req)
		if err != nil {
			return order.Order{}, badRequest(err.Error())
		}
		return created, nil
	}
}
