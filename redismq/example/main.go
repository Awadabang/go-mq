package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Awadabang/go-mq"
	"github.com/Awadabang/go-mq/redismq"
	"github.com/go-redis/redis/v9"
	"golang.org/x/sync/errgroup"
)

func main() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
		DB:    0,
	})
	_, err := rdb.Ping(context.Background()).Result()
	if err != nil {
		log.Panicln(err)
	}

	testP2P(rdb)
	testPubSub(rdb)
}

func testP2P(broker redis.UniversalClient) {
	p2pClient := redismq.NewP2PClient(broker, "example_list")
	go func() {
		res, err := p2pClient.Consume(context.Background())
		if err != nil {
			log.Panicln(err)
		}
		log.Println(res)
	}()
	time.Sleep(time.Second)
	p2pClient.Produce(context.Background(), []any{"aaa", "bbb"})
}

func testPubSub(broker redis.UniversalClient) {
	pubSubClient, err := redismq.NewPubSubClient(broker, "example", "example_group", &mq.PubSubOption{
		From:              "$",
		Consumer:          "example_consumer",
		MaxConsumeCount:   10,
		AutoClaimIdleTime: 30 * time.Minute,
		MaxLen:            1000,
		Approx:            true,
	})
	if err != nil {
		log.Panicln(err)
	}
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			<-ticker.C
			if _, err := pubSubClient.Produce(context.Background(), map[string]any{
				"a": "b",
			}); err != nil {
				log.Println(err)
			}
		}
	}()

	for {
		res, err := pubSubClient.Consume(context.Background())
		if err != nil {
			log.Println(err)
			return
		}

		var (
			ids []string
			mtx sync.Mutex
		)

		eg := errgroup.Group{}
		for _, v := range res {
			data := v
			eg.Go(func() error {
				log.Println("handling data..., id: ", data.Id)
				mtx.Lock()
				ids = append(ids, data.Id)
				mtx.Unlock()
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			log.Panicln(err)
		}

		err = pubSubClient.SendAcks(context.Background(), ids)
		if err != nil {
			log.Panicln(err)
		}
	}
}
