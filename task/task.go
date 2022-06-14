package task

import (
	"time"

	"github.com/google/uuid"
)

type TaskOption struct {
	RetryCount    int
	RetryTimeout  int
	ResultExpired int
	PollInterval  int
	IgnoreResult  bool
	StartAt       *int64
}

type Task struct {
	Option    TaskOption
	Id        string
	Name      string
	Args      []interface{}
	OnSuccess []*Task
	OnFailed  []*Task
	BackOff   *BackOff
}

func NewTaskOption(retryCount int, retryTimeout time.Duration) *TaskOption {
	return &TaskOption{
		RetryCount:    retryCount,
		RetryTimeout:  int(retryTimeout.Milliseconds()),
		IgnoreResult:  false,
		ResultExpired: 15,
		PollInterval:  500,
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

func (to *TaskOption) WithResultExpired(in time.Duration) *TaskOption {
	to.ResultExpired = int(in.Seconds())
	return to
}

func (to *TaskOption) WithPollInterval(timer time.Duration) *TaskOption {
	to.PollInterval = int(timer.Milliseconds())
	return to
}

func NewTask(opt *TaskOption, name string, args ...interface{}) *Task {
	if opt == nil {
		opt = NewTaskOption(1, 3)
	}
	return &Task{
		Option:  *opt,
		Id:      uuid.New().String(),
		Name:    name,
		Args:    args,
		BackOff: newBackOff(time.Duration(opt.RetryTimeout)*time.Millisecond, 1.5),
	}
}
