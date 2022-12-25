package redismq

import "context"

func (r *RedisMqClient) Produce(ctx context.Context, values []any) (int64, error) {
	return r.rdb.LPush(ctx, "", values...).Result()
}
