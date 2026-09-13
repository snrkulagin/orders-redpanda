// Package http exposes order-service's HTTP API.
package http

import (
	"context"
	"net/http"

	"github.com/snrkulagin/orders-redpanda/internal/domain/order"
	orderservice "github.com/snrkulagin/orders-redpanda/internal/service/order"
)

type OrderCreator interface {
	CreateOrder(ctx context.Context, req orderservice.CreateOrderRequest) (order.Order, error)
}

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
