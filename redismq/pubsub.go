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

	xReadGroupArgs := &redis.XReadGroupArgs{
		Group:    group,                // consumer group
		Consumer: opt.Consumer,         // Consumer, created on-the-fly
		Streams:  []string{topic, ">"}, // stream
		Block:    0,                    // infinite waiting
		NoAck:    false,                // Confirmation required
		Count:    1,
	}

	xReadGroupFromEPLArgs := &redis.XReadGroupArgs{
		Group:    group,                // consumer group
		Consumer: opt.Consumer,         // Consumer, created on-the-fly
		Streams:  []string{topic, "0"}, // stream
		Block:    0,                    // infinite waiting
		NoAck:    false,                // Confirmation required
		Count:    1,
	}

	xAutoClaimArgs := &redis.XAutoClaimArgs{
		Start:    "0-0",
		Stream:   topic,
		Group:    group,
		MinIdle:  opt.AutoClaimIdleTime,
		Count:    1,
		Consumer: opt.Consumer,
	}

	return &RedisPubSubClient{
		broker:                broker,
		topic:                 topic,
		group:                 group,
		opt:                   opt,
		xAutoClaimArgs:        xAutoClaimArgs,
		xReadGroupArgs:        xReadGroupArgs,
		xReadGroupFromEPLArgs: xReadGroupFromEPLArgs,
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
		return []*mq.Message{dm}, nil
	}

	if dm, err := r.receiveNextMessage(ctx, r.xReadGroupFromEPLArgs); dm != nil && err == nil {
		return []*mq.Message{dm}, nil
	}

	dm, err := r.receiveNextMessage(ctx, r.xReadGroupArgs)
	if err != nil {
		return nil, err
	}
	return []*mq.Message{dm}, nil
}

func (r *RedisPubSubClient) receiveAutoClaimMessage(ctx context.Context, args *redis.XAutoClaimArgs) (*mq.Message, error) {
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
	return convertMsgFromRredisMsg(&msgs[0])
}

func (r *RedisPubSubClient) receiveNextMessage(ctx context.Context, args *redis.XReadGroupArgs) (*mq.Message, error) {
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
	return convertMsgFromRredisMsg(&xStreamSlice[0].Messages[0])
}

func convertMsgFromRredisMsg(msg *redis.XMessage) (*mq.Message, error) {
	return &mq.Message{
		Id:     msg.ID,
		Values: msg.Values,
	}, nil
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
