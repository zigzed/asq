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

	buf, err := b.opt.Marshaller.EncodeResult(result.Results, result.Error)
	if err != nil {
		return errors.Wrapf(err, "encode result %v for %s, %s failed",
			result.Results, result.Name, result.Id)
	}

	if _, err := b.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.LPush(ctx, key, buf)
		pipe.Expire(ctx, key, result.Timeout)
		return nil
	}); err != nil {
		return errors.Wrapf(err, "push result %v failed", result)
	}

	return nil
}

func (b *backend) Scan(ctx context.Context, id, name string, args ...interface{}) (bool, error) {
	var (
		key = b.makeTaskKeyForBackend(id, name)
		res []string
		err error
	)

	for {
		tmo := 5 * time.Second
		if Deadline, ok := ctx.Deadline(); ok {
			tmo = time.Until(Deadline)
		}

		res, err = b.rdb.BRPop(ctx, tmo, key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return false, err
		}
		break
	}
	if len(res) != 2 {
		return false, errors.Wrapf(err, "unsupported result for %s: %v", name, res)
	}

	ok, err := b.opt.Marshaller.DecodeResult(res[1], args...)
	if !ok {
		return true, errors.Wrapf(err, "unmarshal result for %s, %s, %s failed",
			name, id, res)
	}

	return true, err
}

func (b *backend) Close() error {
	return b.rdb.Close()
}

func (b *backend) makeTaskKeyForBackend(id, name string) string {
	return fmt.Sprintf("{%s}.%s.%s.%s", b.name, "result", name, id)
}
