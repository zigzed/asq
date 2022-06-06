package task

import (
	"time"

	"github.com/gofrs/uuid"
)

type TaskOption struct {
	RetryCount   int
	RetryTimeout int
	IgnoreResult bool
	StartAt      *int64
}

type Task struct {
	Option    TaskOption
	Id        string
	Name      string
	Args      []interface{}
	OnSuccess []*Task
	OnFailed  []*Task
}

func NewTaskOption(retryCount int, retryTimeout time.Duration) *TaskOption {
	return &TaskOption{
		RetryCount:   retryCount,
		RetryTimeout: int(retryTimeout.Milliseconds()),
		IgnoreResult: false,
	}
}

func (to *TaskOption) WithIgnoreResult(ignore bool) *TaskOption {
	to.IgnoreResult = ignore
	return to
}

func (to *TaskOption) WithStartAt(eta time.Time) *TaskOption {
	to.StartAt = new(int64)
	*to.StartAt = eta.UnixMilli()
	return to
}

func NewTask(opt *TaskOption, name string, args ...interface{}) *Task {
	id, _ := uuid.NewV4()
	if opt == nil {
		opt = NewTaskOption(1, 3)
	}
	return &Task{
		Option: *opt,
		Id:     id.String(),
		Name:   name,
		Args:   args,
	}
}
