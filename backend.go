package asq

import (
	"context"

	"github.com/zigzed/asq/result"
)

type Backend interface {
	Push(ctx context.Context, results *result.Result) error
	Poll(ctx context.Context, id, name string) ([]interface{}, bool, error)
}
