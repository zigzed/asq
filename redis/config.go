package redis

import (
	"time"

	"github.com/zigzed/asq/marshaller"
)

type Option struct {
	Addrs            []string
	DB               int
	Username         string
	Password         string
	SentinelUsername string
	SentinelPassword string
	MasterName       string
	Marshaller       marshaller.Marshaller
	PollPeriod       time.Duration
}

func DefaultOption() *Option {
	return &Option{
		Addrs:      []string{"127.0.0.1:6379"},
		Marshaller: marshaller.NewJsonMarshaller(),
		PollPeriod: 100 * time.Millisecond,
	}
}
