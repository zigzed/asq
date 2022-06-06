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
	if opt.PollInterval == 0 {
		opt.PollInterval = 500 * time.Millisecond
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

func (b *broker) Poll(ctx context.Context, name string) (*task.Task, error) {
	return b.doPoll(ctx, name)
}

func (b *broker) Close() error {
	return b.rdb.Close()
}

func (b *broker) doPoll(ctx context.Context, name string) (*task.Task, error) {
	taskKey := b.makeTaskKeyForBroker(name)
	delayed := b.makeDelayedKeyForBroker(name)

	if err := b.moveDelayed(ctx, delayed, taskKey); err != nil {
		return nil, err
	}
	buf, err := b.fetchTasks(ctx, taskKey)
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
	local items = redis.call('ZRANGE', KEYS[1], 0, ARGV[1], 'BYSCORE')
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

func (b *broker) fetchTasks(ctx context.Context, key string) (string, error) {
	reply, err := b.rdb.RPop(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", errors.Wrapf(err, "poll broker %s for %s failed", b.name, key)
	}

	return reply, nil
}

func (b *broker) makeTaskKeyForBroker(name string) string {
	return fmt.Sprintf("%s.%s.{%s}", b.name, "tasks", name)
}

func (b *broker) makeDelayedKeyForBroker(name string) string {
	return fmt.Sprintf("%s.%s.{%s}", b.name, "delayed", name)
}
