package redismq

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
	"github.com/pkg/errors"
)

type ConsumeFunc func(msg *mq.Message) error

type RedisMq struct {
	broker redis.UniversalClient
	group  string

	// producer
	streamMaxLength      int64
	approximateMaxLength bool

	// consumer
	consumerName      string
	visibilityTimeout time.Duration
	blockingTimeout   time.Duration
	reclaimInterval   time.Duration
	bufferSize        int
	concurrency       int
	errors            chan error
	queue             chan *mq.Message
	wg                *sync.WaitGroup
	stopReclaim       chan struct{}
	stopPoll          chan struct{}
	stopWorkers       chan struct{}
	consumers         map[string]ConsumeFunc
	streams           []string
}

type OptionApplier func(*RedisMq)

var Aplr OptionApplier

func (OptionApplier) WithStreamMaxLength(maxLen int64, approx bool) OptionApplier {
	return func(r *RedisMq) {
		r.streamMaxLength = maxLen
		r.approximateMaxLength = approx
	}
}

func (OptionApplier) WithConsumers(consumers map[string]ConsumeFunc) OptionApplier {
	return func(r *RedisMq) {
		r.consumers = consumers
	}
}

func defaultRedisMq() *RedisMq {
	return &RedisMq{
		streamMaxLength:      1000,
		approximateMaxLength: true,
		visibilityTimeout:    60 * time.Second,
		blockingTimeout:      5 * time.Second,
		reclaimInterval:      1 * time.Second,
		bufferSize:           100,
		concurrency:          10,
		queue:                make(chan *mq.Message),
		wg:                   &sync.WaitGroup{},
		stopReclaim:          make(chan struct{}),
		stopPoll:             make(chan struct{}),
		stopWorkers:          make(chan struct{}),
		consumers:            map[string]ConsumeFunc{},
	}
}

func NewMq(broker redis.UniversalClient, group, consumer string, applys ...OptionApplier) (mq.Mq, error) {
	m := defaultRedisMq()
	for _, apply := range applys {
		apply(m)
	}

	_, err := broker.Ping(context.Background()).Result()
	if err != nil {
		return nil, err
	}

	if err := redisPreflightChecks(broker); err != nil {
		return nil, err
	}
	m.broker = broker
	m.group = group
	m.consumerName = consumer

	return m, nil
}

func (r *RedisMq) Produce(ctx context.Context, msg *mq.Message) error {
	args := &redis.XAddArgs{
		ID:     msg.Id,
		Stream: msg.Stream,
		Values: msg.Values,
		MaxLen: r.streamMaxLength,
	}
	if r.approximateMaxLength {
		args.Approx = true
	}
	id, err := r.broker.XAdd(ctx, args).Result()
	if err != nil {
		return err
	}
	msg.Id = id
	return nil
}

func (r *RedisMq) Consume(ctx context.Context, errChan chan error) {
	r.errors = errChan

	if len(r.consumers) == 0 {
		r.errors <- errors.New("at least one consumer function needs to be set")
		return
	}

	for stream := range r.consumers {
		r.streams = append(r.streams, stream)
		err := r.broker.XGroupCreateMkStream(ctx, stream, r.group, "0").Err()
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			r.errors <- errors.Wrap(err, "error creating consumer group")
			return
		}
	}

	for i := 0; i < len(r.consumers); i++ {
		r.streams = append(r.streams, ">")
	}

	go r.reclaim(ctx)
	go r.poll(ctx)

	// shutdown

	r.wg.Add(r.concurrency)
	for i := 0; i < r.concurrency; i++ {
		go r.work(ctx)
	}

	r.wg.Wait()
}

func (r *RedisMq) reclaim(ctx context.Context) {
	if r.visibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(r.reclaimInterval)

	for {
		select {
		case <-r.stopReclaim:
			r.stopPoll <- struct{}{}
			return
		case <-ticker.C:
			for stream := range r.consumers {
				start := "-"
				end := "+"
				for {
					res, err := r.broker.XPendingExt(ctx,
						&redis.XPendingExtArgs{
							Stream:   stream,
							Group:    r.group,
							Consumer: r.consumerName,
							Start:    start,
							End:      end,
							Count:    int64(r.bufferSize - len(r.queue)),
						}).Result()
					if err != nil && err != redis.Nil {
						r.errors <- errors.Wrap(err, "error listing pending messages")
						break
					}

					if len(res) == 0 {
						break
					}

					msgs := make([]string, 0)

					for _, resItem := range res {
						if resItem.Idle >= r.visibilityTimeout {
							claimres, err := r.broker.XClaim(ctx,
								&redis.XClaimArgs{
									Stream:   stream,
									Group:    r.group,
									Consumer: r.consumerName,
									MinIdle:  r.visibilityTimeout,
									Messages: []string{resItem.ID},
								}).Result()
							if err != nil && err != redis.Nil {
								r.errors <- errors.Wrapf(err, "error claiming %d message(s)", len(msgs))
								break
							}
							// If the Redis nil error is returned, it means that
							// the message no longer exists in the stream.
							// However, it is still in a pending state. This
							// could happen if a message was claimed by a
							// consumer, that consumer died, and the message
							// gets deleted (either through a XDEL call or
							// through MAXLEN). Since the message no longer
							// exists, the only way we can get it out of the
							// pending state is to acknowledge it.
							if err == redis.Nil {
								err = r.broker.XAck(ctx, stream, r.group, resItem.ID).Err()
								if err != nil {
									r.errors <- errors.Wrapf(err, "error acknowledging after failed claim for %q stream and %q message", stream, resItem.ID)
									continue
								}
							}
							r.enqueue(stream, claimres)
						}
					}

					newID, err := incrementMessageID(res[len(res)-1].ID)
					if err != nil {
						r.errors <- err
						break
					}

					start = newID
				}
			}
		}
	}
}

func (r *RedisMq) poll(ctx context.Context) {
	for {
		select {
		case <-r.stopPoll:
			// once the polling has stopped (i.e. there will be no more messages
			// put onto c.queue), stop all of the workers
			for i := 0; i < r.concurrency; i++ {
				r.stopWorkers <- struct{}{}
			}
			return
		default:
			res, err := r.broker.XReadGroup(ctx,
				&redis.XReadGroupArgs{
					Group:    r.group,
					Consumer: r.consumerName,
					Streams:  r.streams,
					Count:    int64(r.bufferSize - len(r.queue)),
					Block:    r.blockingTimeout,
				}).Result()
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				if err == redis.Nil {
					continue
				}
				r.errors <- errors.Wrap(err, "error reading redis stream")
				continue
			}

			for _, resItem := range res {
				r.enqueue(resItem.Stream, resItem.Messages)
			}
		}
	}
}

func (r *RedisMq) enqueue(stream string, msgs []redis.XMessage) {
	for _, m := range msgs {
		msg := &mq.Message{
			Id:     m.ID,
			Stream: stream,
			Values: m.Values,
		}
		r.queue <- msg
	}
}

func (r *RedisMq) work(ctx context.Context) {
	defer r.wg.Done()

	for {
		select {
		case msg := <-r.queue:
			err := r.process(msg)
			if err != nil {
				r.errors <- errors.Wrapf(err, "error calling ConsumerFunc for %q stream and %q message", msg.Stream, msg.Id)
				continue
			}
			err = r.broker.XAck(ctx, msg.Stream, r.group, msg.Id).Err()
			if err != nil {
				r.errors <- errors.Wrapf(err, "error acknowledging after success for %q stream and %q message", msg.Stream, msg.Id)
				continue
			}
		case <-r.stopWorkers:
			return
		}
	}
}

func (r *RedisMq) process(msg *mq.Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = errors.Wrap(e, "ConsumerFunc panic")
				return
			}
			err = errors.Errorf("ConsumerFunc panic: %v", r)
		}
	}()
	err = r.consumers[msg.Stream](msg)
	return
}

func (r *RedisMq) Shutdown() {
	r.stopReclaim <- struct{}{}
	if r.visibilityTimeout == 0 {
		r.stopPoll <- struct{}{}
	}
}
