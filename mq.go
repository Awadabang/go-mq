package mq

import (
	"context"
)

type (
	Message struct {
		Id     string
		Stream string
		Values map[string]interface{}
	}

	Mq interface {
		Produce(ctx context.Context, msg *Message) error
		Consume(ctx context.Context, errChan chan error)
		Shutdown()
	}
)
