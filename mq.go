package mq

import (
	"context"
	"time"
)

type (
	P2PMq interface {
		Produce(ctx context.Context, values []any) (int64, error)
		Consume(ctx context.Context) ([]any, error)
	}

	Message struct {
		Id     string
		Values map[string]interface{}
	}

	PubSubMq interface {
		Produce(ctx context.Context, values map[string]any) (string, error)
		Consume(ctx context.Context) ([]*Message, error)
		SendAcks(ctx context.Context, ids []string) error
	}

	PubSubOption struct {
		From              string
		Consumer          string
		MaxConsumeCount   int64
		AutoClaimIdleTime time.Duration
		MaxLen            int64
		Approx            bool
	}
)
