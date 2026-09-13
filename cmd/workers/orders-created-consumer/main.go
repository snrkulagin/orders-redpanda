package main

import (
	"context"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/snrkulagin/orders-redpanda/internal/config"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
	"github.com/snrkulagin/orders-redpanda/internal/worker/orderslogger"
)

func main() {
	if err := config.LoadDotEnv(); err != nil {
		log.Fatalf("config: %v", err)
	}

	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:19092"), ",")
	group := config.String("KAFKA_CONSUMER_GROUP", "orders-created-logger")

	startOffset, err := kafka.ParseStartOffset(config.String("KAFKA_START_OFFSET", "earliest"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	consumer, err := kafka.NewConsumer(brokers, group, startOffset, kafka.TopicOrdersCreated)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	app := orderslogger.New(consumer)

	log.Printf("orders-created-consumer started, group=%s topic=%s start=%s", group, kafka.TopicOrdersCreated, startOffset)

	if err := app.Run(ctx); err != nil {
		log.Fatalf("orders-created-consumer: %v", err)
	}
}
