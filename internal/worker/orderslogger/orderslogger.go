package orderslogger

import (
	"context"
	"log"

	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
)

// App logs every orders.created record it consumes. It owns none of its
// dependencies — main.go creates and closes them.
type App struct {
	consumer *kafka.Consumer
}

// New wires an already-constructed consumer into a runnable App.
func New(consumer *kafka.Consumer) *App {
	return &App{consumer: consumer}
}

// Run blocks, logging every orders.created record until ctx is canceled.
func (a *App) Run(ctx context.Context) error {
	return a.consumer.Consume(ctx, a.handle)
}

func (a *App) handle(_ context.Context, msg kafka.Message) {
	log.Printf("%s: partition=%d offset=%d key=%s value=%s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
}
