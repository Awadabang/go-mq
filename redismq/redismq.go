package redismq

import (
	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
)

type RedisMqClient struct {
	rdb redis.UniversalClient
}

func NewClient() mq.Mq {
	return &RedisMqClient{
		rdb: redis.NewUniversalClient(&redis.UniversalOptions{}),
	}
}
