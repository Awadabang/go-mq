package redismq

import (
	"context"

	"github.com/Awadabang/go-mq"
)

func (r *RedisMqClient) Consume(ctx context.Context) ([]*mq.Message, error) {
	return nil, nil
}
