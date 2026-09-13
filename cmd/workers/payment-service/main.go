package main

import (
	"context"
	"log"
	"os/signal"
	"strings"
	"syscall"

	"github.com/snrkulagin/orders-redpanda/internal/config"
	"github.com/snrkulagin/orders-redpanda/internal/repo"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
	"github.com/snrkulagin/orders-redpanda/internal/service/payment"
	paymentworker "github.com/snrkulagin/orders-redpanda/internal/worker/payment"
)

func main() {
	if err := config.LoadDotEnv(); err != nil {
		log.Fatalf("config: %v", err)
	}

	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:19092"), ",")
	group := config.String("KAFKA_CONSUMER_GROUP", "payment-service")

	startOffset, err := kafka.ParseStartOffset(config.String("KAFKA_START_OFFSET", "earliest"))
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	pgDSN := repo.DSN(
		config.String("POSTGRES_USER", "app"),
		config.String("POSTGRES_PASSWORD", "app"),
		config.String("POSTGRES_HOST", "localhost"),
		config.String("POSTGRES_PORT", "5432"),
		config.String("POSTGRES_DB", "app"),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	pg, err := repo.NewPostgres(ctx, pgDSN)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pg.Close()

	processedOrders := repo.NewProcessedOrders(pg)
	if err := processedOrders.EnsureSchema(ctx); err != nil {
		log.Fatalf("postgres: %v", err)
	}

	paymentSvc := payment.NewService(producer, processedOrders)

	consumer, err := kafka.NewConsumer(brokers, group, startOffset, kafka.TopicOrdersCreated)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	app := paymentworker.New(consumer, paymentSvc)

	log.Printf("payment-service started, group=%s topic=%s start=%s", group, kafka.TopicOrdersCreated, startOffset)

	if err := app.Run(ctx); err != nil {
		log.Fatalf("payment-service: %v", err)
	}
}
