package redismq

import (
	"context"
	"strings"
	"time"

	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
)

type RedisPubSubClient struct {
	broker                redis.UniversalClient
	topic                 string
	group                 string
	opt                   *mq.PubSubOption
	xAutoClaimArgs        *redis.XAutoClaimArgs
	xReadGroupArgs        *redis.XReadGroupArgs
	xReadGroupFromEPLArgs *redis.XReadGroupArgs
}

func NewPubSubClient(broker redis.UniversalClient, topic, group string, opt *mq.PubSubOption) (mq.PubSubMq, error) {
	if opt == nil {
		opt = &mq.PubSubOption{}
	}
	if opt.MaxConsumeCount == 0 {
		opt.MaxConsumeCount = 1
	}
	if opt.From == "" {
		opt.From = "0"
	}
	if opt.AutoClaimIdleTime == 0 {
		opt.AutoClaimIdleTime = 30 * time.Second
	}
	if opt.MaxLen == 0 {
		opt.MaxLen = 1000
	}

	_, err := broker.XGroupCreateMkStream(context.Background(), topic, group, opt.From).Result()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	_, err = broker.XGroupCreateConsumer(context.Background(), topic, group, opt.Consumer).Result()
	if err != nil {
		return nil, err
	}

	xAutoClaimArgs := &redis.XAutoClaimArgs{
		Stream:   topic,
		Group:    group,
		MinIdle:  opt.AutoClaimIdleTime,
		Start:    "0-0",
		Count:    opt.MaxConsumeCount,
		Consumer: opt.Consumer,
	}

	xReadGroupFromEPLArgs := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: opt.Consumer,
		Streams:  []string{topic, "0"},
		Block:    0,
		NoAck:    false,
		Count:    opt.MaxConsumeCount,
	}

	xReadGroupArgs := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: opt.Consumer,
		Streams:  []string{topic, ">"},
		Block:    0,
		NoAck:    false,
		Count:    opt.MaxConsumeCount,
	}

	return &RedisPubSubClient{
		broker:                broker,
		topic:                 topic,
		group:                 group,
		opt:                   opt,
		xAutoClaimArgs:        xAutoClaimArgs,
		xReadGroupFromEPLArgs: xReadGroupFromEPLArgs,
		xReadGroupArgs:        xReadGroupArgs,
	}, nil
}

func (r *RedisPubSubClient) Produce(ctx context.Context, values map[string]any) (string, error) {
	return r.broker.XAdd(ctx, &redis.XAddArgs{
		Stream: r.topic,
		MaxLen: r.opt.MaxLen,
		Approx: r.opt.Approx,
		Values: values,
	}).Result()
}

func (r *RedisPubSubClient) Consume(ctx context.Context) ([]*mq.Message, error) {
	if dm, err := r.receiveAutoClaimMessage(ctx, r.xAutoClaimArgs); dm != nil && err == nil {
		return dm, nil
	}

	if dm, err := r.receiveNextMessage(ctx, r.xReadGroupFromEPLArgs); dm != nil && err == nil {
		return dm, nil
	}

	dm, err := r.receiveNextMessage(ctx, r.xReadGroupArgs)
	if err != nil {
		return nil, err
	}
	return dm, nil
}

func (r *RedisPubSubClient) receiveAutoClaimMessage(ctx context.Context, args *redis.XAutoClaimArgs) ([]*mq.Message, error) {
	msgs, _, err := r.broker.XAutoClaim(ctx, args).Result()
	if err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return convertMsgFromRredisMsg(msgs)
}

func (r *RedisPubSubClient) receiveNextMessage(ctx context.Context, args *redis.XReadGroupArgs) ([]*mq.Message, error) {
	xStreamSlice, err := r.broker.XReadGroup(ctx, args).Result()
	if err != nil || ctx.Err() != nil {
		if err == nil {
			err = ctx.Err()
		}
		return nil, err
	}
	if len(xStreamSlice) == 0 || len(xStreamSlice[0].Messages) == 0 {
		return nil, nil
	}
	return convertMsgFromRredisMsg(xStreamSlice[0].Messages)
}

func convertMsgFromRredisMsg(msgs []redis.XMessage) ([]*mq.Message, error) {
	res := make([]*mq.Message, 0, len(msgs))
	for _, msg := range msgs {
		res = append(res, &mq.Message{
			Id:     msg.ID,
			Values: msg.Values,
		})
	}
	return res, nil
}

func (r *RedisPubSubClient) SendAcks(ctx context.Context, ids []string) error {
	for _, id := range ids {
		_, err := r.broker.XAck(ctx, r.topic, r.group, id).Result()
		if err != nil || ctx.Err() != nil {
			if err == nil {
				err = ctx.Err()
			}
			return err
		}
	}
	return nil
}
