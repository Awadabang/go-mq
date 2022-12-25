package redismq

import (
	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
)

type RedisListClient struct {
	rdb redis.UniversalClient
}

type RedisPubSubClient struct {
	rdb redis.UniversalClient
}

func NewListClient(opt redis.UniversalOptions) mq.Mq {
	return &RedisListClient{
		rdb: redis.NewUniversalClient(&opt),
	}
}

func NewPubSubClient(opt redis.UniversalOptions) mq.Mq {
	return &RedisPubSubClient{
		rdb: redis.NewUniversalClient(&opt),
	}
}
