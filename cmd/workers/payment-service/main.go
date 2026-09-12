package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/snrkulagin/orders-redpanda/internal/config"
	orderdomain "github.com/snrkulagin/orders-redpanda/internal/domain/order"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
	"github.com/snrkulagin/orders-redpanda/internal/service/payment"
)

// simulatedProcessingDelay slows down each record on purpose.
const simulatedProcessingDelay = 500 * time.Millisecond

func main() {
	if err := config.LoadDotEnv(); err != nil {
		log.Fatalf("config: %v", err)
	}

	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:19092"), ",")
	group := config.String("KAFKA_CONSUMER_GROUP", "payment-service")

	startOffsetRaw := config.String("KAFKA_START_OFFSET", "earliest")
	startOffset, err := kafka.ParseStartOffset(startOffsetRaw)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	paymentService := payment.NewService(producer)

	consumer, err := kafka.NewConsumer(brokers, group, startOffset, kafka.TopicOrdersCreated)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf("payment-service started, group=%s topic=%s start=%s", group, kafka.TopicOrdersCreated, startOffsetRaw)

	err = consumer.Consume(ctx, func(ctx context.Context, msg kafka.Message) {
		time.Sleep(simulatedProcessingDelay)

		var o orderdomain.Order
		if err := json.Unmarshal(msg.Value, &o); err != nil {
			log.Printf("payment-service: bad orders.created payload at partition=%d offset=%d: %v",
				msg.Partition, msg.Offset, err)
			return
		}

		result, err := paymentService.ProcessPayment(ctx, o.OrderID)
		if err != nil {
			log.Printf("payment-service: process payment for order_id=%s: %v", o.OrderID, err)
			return
		}

		log.Printf("payment-service: order_id=%s status=%s", result.OrderID, result.Status)
	})
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
}
