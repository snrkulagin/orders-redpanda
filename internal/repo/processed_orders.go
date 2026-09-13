package repo

import (
	"context"
	"fmt"
)

// ProcessedOrders tracks which order IDs payment-service has already
// produced a payment outcome for. Kafka only guarantees at-least-once
// delivery: after a consumer-group rebalance the same orders.created
// record can be redelivered to whichever instance now owns the partition.
// Without this check that redelivery would run ProcessPayment a second
// time and publish a second — possibly different, since the outcome is
// random — payment event for the same order.
type ProcessedOrders struct {
	pg *Postgres
}

// NewProcessedOrders builds a ProcessedOrders repo on top of pg.
func NewProcessedOrders(pg *Postgres) *ProcessedOrders {
	return &ProcessedOrders{pg: pg}
}

// EnsureSchema creates the backing table if it doesn't exist yet. Good
// enough for a learning project; a real service would use a migration
// tool instead of doing this on every startup.
func (r *ProcessedOrders) EnsureSchema(ctx context.Context) error {
	_, err := r.pg.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS processed_orders (
			order_id     TEXT PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`)
	if err != nil {
		return fmt.Errorf("repo: ensure processed_orders schema: %w", err)
	}
	return nil
}

// MarkProcessed atomically records orderID as processed and reports
// whether this call is the first time orderID has been seen (true) or it
// was already processed before (false). The INSERT ... ON CONFLICT makes
// the check-and-set a single round trip, so two instances racing on the
// same orderID can't both see "first time".
func (r *ProcessedOrders) MarkProcessed(ctx context.Context, orderID string) (bool, error) {
	tag, err := r.pg.pool.Exec(ctx,
		`INSERT INTO processed_orders (order_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		orderID,
	)
	if err != nil {
		return false, fmt.Errorf("repo: mark order processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
