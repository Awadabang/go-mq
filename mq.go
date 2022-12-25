package mq

import "context"

type (
	Mq interface {
		Produce(ctx context.Context, values []any) (int64, error)
		Consume(ctx context.Context) ([]*Message, error)
	}

	Message struct {
	}
)
