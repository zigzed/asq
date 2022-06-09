package asq

import (
	"context"

	"github.com/zigzed/asq/result"
)

type Backend interface {
	Push(ctx context.Context, results *result.Result) error
	Scan(ctx context.Context, id, name string, args ...interface{}) (bool, error)
}
