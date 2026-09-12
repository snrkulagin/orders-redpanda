package order

import "time"

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type Order struct {
	OrderID   string    `json:"order_id"`
	UserID    int64     `json:"user_id"`
	Items     []Item    `json:"items"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
