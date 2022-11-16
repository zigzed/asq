package asq

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/cheekybits/is"
	"github.com/golang/glog"
	"github.com/zigzed/asq/redis"
	"github.com/zigzed/asq/task"
)

type TestStruct1 struct {
	A int
	B string
}

type TestStruct2 struct {
	TestStruct1
	C string
}

type TestStruct3 struct {
	A int
	B string
	C time.Duration
}

type TestStruct4 struct {
	E1 TestStruct1
	X  int
	Y  string
}

type TestStruct5 struct {
	E1 TestStruct3
	X  int
	Y  string
}

type TestStruct6 struct {
	A int
	B string
	C struct {
		X int
		Y string
	}
}

type TestStruct7 struct {
	A int
	B string
	C struct {
		X int
		Y string
		Z float64
	}
}

func testFunc1(arg1 *TestStruct1) error {
	fmt.Printf("testFunc1 %v\n", *arg1)
	return nil
}

func testFunc4(arg1 TestStruct4) error {
	fmt.Printf("testFunc4 %v\n", arg1)
	return nil
}

func testFunc6(arg1 TestStruct6) error {
	fmt.Printf("testFunc6 %v\n", arg1)
	return nil
}

func TestAsq2(t *testing.T) {
	flag.Parse()
	defer glog.Flush()

	is := is.New(t)
	app, err := NewAppFromRedis(redis.Option{
		Addrs: []string{"192.168.0.114:13500", "192.168.0.114:13501", "192.168.0.114:13502"},
		// Addrs:      []string{"192.168.0.114:6379"},
		// Addrs:      []string{"127.0.0.1:6379"},
		PollPeriod: 100 * time.Millisecond,
	}, "test")
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		app.StartWorker(ctx, 2)
	}()

	app.Register("testFunc1", testFunc1)
	app.Register("testFunc4", testFunc4)
	app.Register("testFunc6", testFunc6)

	ar, err := app.SubmitTask(ctx,
		task.NewTask(nil, "testFunc1", &TestStruct2{
			TestStruct1: TestStruct1{
				A: 1,
				B: "1",
			},
			C: "2",
		}))
	is.NoErr(err)
	ok, err := ar.Wait(ctx)
	is.True(ok)
	is.NoErr(err)

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testFunc1", &TestStruct3{
			A: 1,
			B: "1",
			C: 1 * time.Second,
		}))
	is.NoErr(err)
	ok, err = ar.Wait(ctx)
	is.True(ok)
	is.NoErr(err)

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testFunc4", TestStruct5{
			E1: TestStruct3{
				A: 4,
				B: "4",
				C: 4 * time.Second,
			},
			X: 4,
			Y: "4",
		}))
	is.NoErr(err)
	ok, err = ar.Wait(ctx)
	is.True(ok)
	is.NoErr(err)

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testFunc6", TestStruct7{
			A: 7,
			B: "7",
			C: struct {
				X int
				Y string
				Z float64
			}{
				X: 7,
				Y: "7",
				Z: 7.0,
			},
		}))
	is.NoErr(err)
	ok, err = ar.Wait(ctx)
	is.True(ok)
	is.NoErr(err)
}
