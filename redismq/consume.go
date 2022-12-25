package redismq

import (
	"context"

	"github.com/Awadabang/go-mq"
)

func (r *RedisListClient) Consume(ctx context.Context) ([]*mq.Message, error) {
	return nil, nil
}

func (r *RedisPubSubClient) Consume(ctx context.Context) ([]*mq.Message, error) {
	return nil, nil
}
