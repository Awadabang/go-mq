package redismq

import "context"

func (r *RedisListClient) Produce(ctx context.Context, values []any) (int64, error) {
	return r.rdb.LPush(ctx, "", values...).Result()
}

func (r *RedisPubSubClient) Produce(ctx context.Context, values []any) (int64, error) {
	return r.rdb.LPush(ctx, "", values...).Result()
}
