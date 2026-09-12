package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/snrkulagin/orders-redpanda/internal/config"
	"github.com/snrkulagin/orders-redpanda/internal/service/kafka"
	"github.com/snrkulagin/orders-redpanda/internal/service/order"
	ordertransport "github.com/snrkulagin/orders-redpanda/internal/transport/http"
)

func main() {
	if err := config.LoadDotEnv(); err != nil {
		log.Fatalf("config: %v", err)
	}

	brokers := strings.Split(config.String("KAFKA_BROKERS", "localhost:19092"), ",")
	httpAddr := config.String("HTTP_ADDR", ":8081")

	producer, err := kafka.NewProducer(brokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	orderService := order.NewService(producer)

	srv := &http.Server{
		Addr:    httpAddr,
		Handler: ordertransport.NewRouter(orderService),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("order-service listening on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
