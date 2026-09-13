package repo

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres wraps a connection pool to the orders Postgres database. It is
// the shared handle that individual repos (ProcessedOrders, and whatever
// follows) are built on top of.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres opens a connection pool for dsn and verifies it with a ping.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("repo: open postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("repo: ping postgres: %w", err)
	}

	return &Postgres{pool: pool}, nil
}

// Close releases all connections in the pool.
func (p *Postgres) Close() {
	p.pool.Close()
}

// DSN builds a postgres:// connection string from its parts.
func DSN(user, password, host, port, db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, db)
}
