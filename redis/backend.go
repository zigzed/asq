package redis

import (
	"context"
	"fmt"
	"time"

	"emperror.dev/errors"
	"github.com/go-redis/redis/v8"
	"github.com/zigzed/asq/marshaller"
	"github.com/zigzed/asq/result"
)

type backend struct {
	rdb  redis.UniversalClient
	opt  Option
	name string
}

func NewBackend(opt *Option, queueName string) (*backend, error) {
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

	return &backend{
		rdb:  rdb,
		opt:  *opt,
		name: queueName,
	}, nil
}

func (b *backend) Push(ctx context.Context, result *result.Result) error {
	key := b.makeTaskKeyForBackend(result.Id, result.Name)

	buf, err := b.opt.Marshaller.EncodeResult(result.Results)
	if err != nil {
		return errors.Wrapf(err, "encode result %v for %s, %s failed",
			result.Results, result.Name, result.Id)
	}

	_, err = b.rdb.Set(ctx, key, buf, result.Timeout).Result()
	return err
}

func (b *backend) Poll(ctx context.Context, id, name string) ([]interface{}, bool, error) {
	key := b.makeTaskKeyForBackend(id, name)

	res, err := b.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, nil
	}

	results, err := b.opt.Marshaller.DecodeResult(res)
	if err != nil {
		return nil, false, errors.Wrapf(err, "unmarshal result for %s, %s, %s failed",
			name, id, res)
	}

	b.rdb.Del(ctx, key)

	return results, true, nil
}

func (b *backend) Close() error {
	return b.rdb.Close()
}

func (b *backend) makeTaskKeyForBackend(id, name string) string {
	return fmt.Sprintf("%s.%s.{%s}.%s", b.name, "result", name, id)
}
