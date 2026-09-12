package main

import (
	"context"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/snrkulagin/orders-redpanda/internal/config"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
)

func main() {
	if err := config.LoadDotEnv(); err != nil {
		log.Fatalf("config: %v", err)
	}

	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:19092"), ",")
	group := config.String("KAFKA_CONSUMER_GROUP", "orders-created-logger")

	startOffsetRaw := config.String("KAFKA_START_OFFSET", "earliest")
	startOffset, err := kafka.ParseStartOffset(startOffsetRaw)
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

	log.Printf("orders-created-consumer started, group=%s topic=%s start=%s", group, kafka.TopicOrdersCreated, startOffsetRaw)

	err = consumer.Consume(ctx, func(_ context.Context, msg kafka.Message) {
		log.Printf("%s: partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
	})
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
}
