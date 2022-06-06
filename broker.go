package asq

import (
	"context"

	"github.com/zigzed/asq/task"
)

type Broker interface {
	Push(ctx context.Context, task *task.Task) error
	Poll(ctx context.Context, name string) (*task.Task, error)
}
