package redis

import (
	"testing"
)

func TestRedisBroker(t *testing.T) {
	// is := is.New(t)

	// b, err := NewBroker(&Option{
	// 	Addrs:        []string{"127.0.0.1:6379"},
	// 	PollInterval: 500 * time.Millisecond,
	// },
	// 	"test")
	// is.NoErr(err)

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// event := make(chan error, 8)
	// tasks := make(chan task.Task, 32)
	// err = b.Poll(ctx, "taskA", tasks, event)
	// is.NoErr(err)

	// go func() {
	// 	for {
	// 		select {
	// 		case task := <-tasks:
	// 			fmt.Printf("%s task: %+v\n",
	// 				time.Now().Format("20060102 150405.000"),
	// 				task)
	// 		case err := <-event:
	// 			fmt.Printf("%s errs: %v\n",
	// 				time.Now().Format("20060102 150405.000"),
	// 				err)
	// 		}
	// 	}
	// }()

	// err = b.Push(ctx, asq.NewTask(
	// 	asq.NewTaskOption(1, 3*time.Second),
	// 	"taskA",
	// 	1, "3"))
	// is.NoErr(err)

	// err = b.Push(ctx, asq.NewTask(
	// 	asq.NewTaskOption(1, 3*time.Second).
	// 		WithIgnoreResult(true).
	// 		WithStartAt(time.Now().Add(5*time.Second)),
	// 	"taskA",
	// 	2, "6"))
	// is.NoErr(err)

	// time.Sleep(30 * time.Second)
}
