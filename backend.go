package asq

import (
	"context"

	"github.com/zigzed/asq/result"
)

type Backend interface {
	Push(ctx context.Context, results result.Result) error
	// Poll(ctx context.Context, name string, task chan<- Task, event chan<- error) error
}
