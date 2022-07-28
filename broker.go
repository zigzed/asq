package asq

import (
	"context"
	"time"

	"github.com/zigzed/asq/task"
)

type Broker interface {
	Push(ctx context.Context, task *task.Task) error
	Poll(ctx context.Context, timeout time.Duration) (*task.Task, error)
}
