package redis

import (
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
}

func DefaultOption() *Option {
	return &Option{
		Addrs:      []string{"127.0.0.1:6379"},
		Marshaller: marshaller.NewJsonMarshaller(),
	}
}
