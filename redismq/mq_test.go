package redismq

import (
	"context"
	"testing"

	"github.com/Awadabang/go-mq"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMq(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	t.Run("new Mq", func(t *testing.T) {
		mq, err := NewMq(rdb, "example_group", "example_consumer")
		require.NoError(t, err)
		assert.NotNil(t, mq)
	})
}

func TestRedisMq_Produce(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{})
	m, err := NewMq(rdb, "example_group", "example_consumer",
		Aplr.WithStreamMaxLength(100, true),
	)
	require.NoError(t, err)

	t.Run("produce", func(t *testing.T) {
		err := m.Produce(context.Background(), &mq.Message{
			Id:     "1",
			Stream: "example_stream",
			Values: map[string]interface{}{
				"a": "b",
			},
		})
		require.NoError(t, err)
	})
}

func TestRedisMq_Consume(t *testing.T) {
	t.Run("sends an error if no ConsumerFuncs are set", func(tt *testing.T) {
		rdb := redis.NewClient(&redis.Options{})
		m, err := NewMq(rdb, "example_group", "example_consumer",
			Aplr.WithStreamMaxLength(100, true),
		)
		require.NoError(t, err)

		errCh := make(chan error)
		go func() {
			err := <-errCh
			require.Error(tt, err)
			assert.Equal(tt, "at least one consumer function needs to be set", err.Error())
		}()
		m.Consume(context.Background(), errCh)
	})

	t.Run("calls the ConsumerFunc on for a message", func(tt *testing.T) {
		rdb := redis.NewClient(&redis.Options{})
		var m mq.Mq
		m, err := NewMq(rdb, "example_group", "example_consumer",
			Aplr.WithStreamMaxLength(100, true),
			Aplr.WithConsumers(map[string]ConsumeFunc{
				tt.Name(): func(msg *mq.Message) error {
					assert.Equal(tt, "value", msg.Values["test"])
					m.Shutdown()
					return nil
				},
			}),
		)
		require.NoError(t, err)

		// enqueue a message
		err = m.Produce(context.Background(), &mq.Message{
			Stream: tt.Name(),
			Values: map[string]interface{}{"test": "value"},
		})
		require.NoError(tt, err)

		errCh := make(chan error)
		go func() {
			err := <-errCh
			require.NoError(tt, err)
		}()
		m.Consume(context.Background(), errCh)
	})
}
