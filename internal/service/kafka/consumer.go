package kafka

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is a single record consumed from a topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// StartOffset selects where a consumer group starts reading a topic the
// very first time it consumes it, i.e. before it has ever committed an
// offset for a partition. Once a group has committed, this has no effect —
// the committed offset always wins.
type StartOffset int

const (
	// StartOffsetEarliest replays the topic's full retained history. This
	// is the default.
	StartOffsetEarliest StartOffset = iota
	// StartOffsetLatest skips history: only messages produced after the
	// group first subscribes are delivered.
	StartOffsetLatest
)

// ParseStartOffset parses "earliest" or "latest" (case-insensitive),
// defaulting to StartOffsetEarliest for an empty string.
func ParseStartOffset(s string) (StartOffset, error) {
	switch strings.ToLower(s) {
	case "", "earliest":
		return StartOffsetEarliest, nil
	case "latest":
		return StartOffsetLatest, nil
	default:
		return 0, fmt.Errorf("kafka: invalid start offset %q (want %q or %q)", s, "earliest", "latest")
	}
}

func (s StartOffset) String() string {
	if s == StartOffsetLatest {
		return "latest"
	}
	return "earliest"
}

func (s StartOffset) kgoOffset() kgo.Offset {
	if s == StartOffsetLatest {
		return kgo.NewOffset().AtEnd()
	}
	return kgo.NewOffset().AtStart()
}

// Consumer reads events from Kafka/Redpanda as part of a consumer group.
type Consumer struct {
	client *kgo.Client
}

// NewConsumer creates a consumer in the given group, subscribed to topics.
// start only matters the first time this group ever consumes these topics.
func NewConsumer(brokers []string, group string, start StartOffset, topics ...string) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(start.kgoOffset()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new consumer: %w", err)
	}

	return &Consumer{client: client}, nil
}

// Close releases the underlying connections.
func (c *Consumer) Close() {
	c.client.Close()
}

// Consume blocks, invoking handler for every message fetched from the
// subscribed topics and committing offsets after each batch. It returns nil
// when ctx is canceled, or an error if committing offsets fails.
func (c *Consumer) Consume(ctx context.Context, handler func(context.Context, Message)) error {
	for {
		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}

		for _, fetchErr := range fetches.Errors() {
			log.Printf("kafka: fetch error topic=%s partition=%d: %v",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			handler(ctx, Message{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Key:       r.Key,
				Value:     r.Value,
			})
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("kafka: commit offsets: %w", err)
		}
	}
}
