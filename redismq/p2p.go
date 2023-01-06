package redismq

import (
	"context"

	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
)

type RedisP2PClient struct {
	broker redis.UniversalClient
	topic  string
}

func NewP2PClient(broker redis.UniversalClient, topic string) mq.P2PMq {
	return &RedisP2PClient{
		broker: broker,
		topic:  topic,
	}
}

func (r *RedisP2PClient) Produce(ctx context.Context, values []any) (int64, error) {
	return r.broker.LPush(ctx, r.topic, values...).Result()
}

func (r *RedisP2PClient) Consume(ctx context.Context) ([]any, error) {
	res, err := r.broker.BRPop(ctx, 0, r.topic).Result()
	if err != nil {
		return nil, err
	}
	return []any{res[1]}, nil
}
