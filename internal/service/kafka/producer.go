package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes events to Kafka/Redpanda topics.
type Producer struct {
	client *kgo.Client
}

// NewProducer creates a producer seeded with the given brokers.
func NewProducer(brokers []string) (*Producer, error) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka: new producer: %w", err)
	}

	return &Producer{client: client}, nil
}

// Close releases the underlying connections.
func (p *Producer) Close() {
	p.client.Close()
}

// Publish sends value to topic synchronously. key drives partitioning:
// records sharing the same key always land on the same partition and keep
// their relative order.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	res := p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value})
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("kafka: publish to %s: %w", topic, err)
	}

	return nil
}
