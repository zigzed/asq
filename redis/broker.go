package redis

import (
	"context"
	"fmt"
	"time"

	"emperror.dev/errors"
	"github.com/go-redis/redis/v8"
	"github.com/zigzed/asq/marshaller"
	"github.com/zigzed/asq/task"
)

type broker struct {
	rdb  redis.UniversalClient
	opt  Option
	name string
}

func NewBroker(opt *Option, queueName string) (*broker, error) {
	if opt == nil {
		opt = DefaultOption()
	}
	if opt.Marshaller == nil {
		opt.Marshaller = marshaller.NewJsonMarshaller()
	}

	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:            opt.Addrs,
		DB:               opt.DB,
		Username:         opt.Username,
		Password:         opt.Password,
		SentinelUsername: opt.SentinelUsername,
		SentinelPassword: opt.SentinelPassword,
		MasterName:       opt.MasterName,
	})

	if _, err := rdb.Ping(context.Background()).Result(); err != nil {
		return nil, errors.Wrapf(err, "redis connection of %v failed", opt)
	}

	return &broker{
		rdb:  rdb,
		opt:  *opt,
		name: queueName,
	}, nil
}

func (b *broker) Push(ctx context.Context, task *task.Task) error {
	key := b.makeTaskKeyForBroker(task.Name)
	buf, err := b.opt.Marshaller.EncodeTask(task)
	if err != nil {
		return errors.Wrapf(err, "encode task %v failed", task)
	}

	if task.Option.StartAt == nil {
		if _, err := b.rdb.LPush(ctx, key, buf).Result(); err != nil {
			return errors.Wrapf(err, "broker push %s, %s with %s failed",
				task.Name, task.Id, buf)
		}
	} else {
		if _, err := b.rdb.ZAdd(ctx, b.makeDelayedKeyForBroker(task.Name), &redis.Z{
			Member: buf,
			Score:  float64(*task.Option.StartAt),
		}).Result(); err != nil {
			return errors.Wrapf(err, "broker push %s, %s with %s failed",
				task.Name, task.Id, buf)
		}
	}

	return nil
}

func (b *broker) Poll(ctx context.Context, timeout time.Duration, names ...string) (*task.Task, error) {
	return b.doPoll(ctx, timeout, names...)
}

func (b *broker) Close() error {
	return b.rdb.Close()
}

func (b *broker) doPoll(ctx context.Context, timeout time.Duration, names ...string) (*task.Task, error) {
	taskKeys := make([]string, 0, len(names))
	for _, name := range names {
		taskKey := b.makeTaskKeyForBroker(name)
		delayed := b.makeDelayedKeyForBroker(name)
		taskKeys = append(taskKeys, taskKey)

		if err := b.moveDelayed(ctx, delayed, taskKey); err != nil {
			return nil, err
		}
	}
	buf, err := b.fetchTasks(ctx, timeout, taskKeys...)
	if err != nil {
		return nil, err
	}
	if buf == "" {
		return nil, nil
	}

	if task, err := b.opt.Marshaller.DecodeTask(buf); err != nil {
		return nil, errors.Wrapf(err, "unmarshal task %s failed", buf)
	} else {
		return task, nil
	}
}

func (b *broker) moveDelayed(ctx context.Context, delayed, tasks string) error {
	script := `
	local items = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1])
	for k, v in pairs(items) do
		redis.call('LPUSH', KEYS[2], v)
		redis.call('ZREM', KEYS[1], v)
	end
	`
	_, err := b.rdb.Eval(ctx,
		script,
		[]string{delayed, tasks},
		time.Now().UnixMilli()).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "move broker %s for delayed %s to %s failed",
			b.name, delayed, tasks)
	}
	return nil
}

func (b *broker) fetchTasks(ctx context.Context, timeout time.Duration, keys ...string) (string, error) {
	reply, err := b.rdb.BRPop(ctx, timeout, keys...).Result()
	if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", nil
	}
	if err != nil {
		return "", errors.Wrapf(err, "poll broker %s for %v failed", b.name, keys)
	}

	if len(reply) != 2 {
		return "", errors.Wrapf(err, "poll broker %s for %v reply failed: %v", b.name, keys, reply)
	}

	return reply[1], nil
}

func (b *broker) makeTaskKeyForBroker(name string) string {
	return fmt.Sprintf("%s.%s.{%s}", b.name, "tasks", name)
}

func (b *broker) makeDelayedKeyForBroker(name string) string {
	return fmt.Sprintf("%s.%s.{%s}", b.name, "delayed", name)
}
