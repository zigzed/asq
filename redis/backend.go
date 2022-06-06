package redis

import (
	"context"

	"emperror.dev/errors"
	"github.com/go-redis/redis/v8"
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

func (b *backend) Push(ctx context.Context, result result.Result) error {
	return nil
}

func (b *backend) Close() error {
	return b.rdb.Close()
}
