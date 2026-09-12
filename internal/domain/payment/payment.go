package payment

import "time"

type Status string

const (
	StatusPaid   Status = "paid"
	StatusFailed Status = "failed"
)

type Payment struct {
	OrderID     string    `json:"order_id"`
	Status      Status    `json:"status"`
	ProcessedAt time.Time `json:"processed_at"`
}
