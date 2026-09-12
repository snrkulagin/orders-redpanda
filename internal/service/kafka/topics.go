package kafka

const (
	// TopicOrdersCreated carries newly created orders, keyed by order_id.
	TopicOrdersCreated = "orders.created"

	// TopicOrdersPaymentProcessed carries the outcome of processing payment
	// for an order - keyed by order_id.
	TopicOrdersPaymentProcessed = "orders.payment_processed"
)
