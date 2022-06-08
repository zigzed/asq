package asq

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"testing"
	"time"

	"emperror.dev/errors"
	"github.com/cheekybits/is"
	"github.com/zigzed/asq/redis"
	"github.com/zigzed/asq/task"
)

func TestMakeAppLink(t *testing.T) {
	t1 := task.NewTask(nil, "a", 1)
	t2 := task.NewTask(nil, "b", 2)
	t3 := task.NewTask(nil, "c", 3)

	app := App{}
	x1 := app.makeTaskLink(t1, t2, t3)

	x := x1
	for n := 1; x != nil; n++ {
		for i := 0; i < n*2; i++ {
			fmt.Print(" ")
		}
		fmt.Printf("taskLink: %+v\n", x)
		if len(x.OnSuccess) > 0 {
			x = x.OnSuccess[0]
		} else {
			x = nil
		}
	}
}

func testA() error {
	fmt.Printf("  in testA done\n")
	return nil
}

func testB(i int) error {
	fmt.Printf("  in testB(%d) done\n", i)
	return nil
}

func testC(i int) (int, error) {
	fmt.Printf("  in testC(%d) %d done\n", i, i*2)
	return i * 2, nil
}

type SimpleStruct struct {
	A string
	B int
	C struct {
		M string
		N sql.NullString
		P *int
	}
}

func testD(v SimpleStruct) error {
	fmt.Printf("  in testD(%+v) done\n", v)
	return nil
}

var testEv = 3

func testE(v *int) (*SimpleStruct, error) {
	testEv--
	if testEv > 0 {
		fmt.Printf("  inTestE(%v) error\n", testEv)
		return nil, errors.Errorf("test error %d", testEv)
	}

	fmt.Printf("  inTestE(%v) done\n", testEv)
	return &SimpleStruct{
		A: "result_A",
		B: testEv,
		C: struct {
			M string
			N sql.NullString
			P *int
		}{
			M: "result_M",
			P: v,
		},
	}, nil
}

func TestAsq(t *testing.T) {
	flag.Parse()

	is := is.New(t)
	broker, err := redis.NewBroker(&redis.Option{
		Addrs: []string{"127.0.0.1:6379"},
	}, "test")
	is.NoErr(err)

	backend, err := redis.NewBackend(&redis.Option{
		Addrs: []string{"127.0.0.1:6379"},
	}, "test")
	is.NoErr(err)

	app := NewApp(broker, backend)

	err = app.Register("testA", testA)
	is.NoErr(err)
	err = app.Register("testB", testB)
	is.NoErr(err)
	err = app.Register("testC", testC)
	is.NoErr(err)
	err = app.Register("testD", testD)
	is.NoErr(err)
	err = app.Register("testE", testE)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err = app.StartWorker(ctx, 1)
		is.NoErr(err)
	}()

	// err = app.SubmitTask(ctx, task.NewTask(nil, "testA"))
	// is.NoErr(err)

	// err = app.SubmitTask(ctx, task.NewTask(nil, "testB", 3))
	// is.NoErr(err)

	// err = app.SubmitTask(ctx, task.NewTask(nil, "testC", 30))
	// is.NoErr(err)

	// err = app.SubmitTask(ctx, task.NewTask(nil, "testD", simpleStruct{
	// 	A: "a",
	// 	B: 1,
	// 	C: struct {
	// 		M string
	// 		N sql.NullString
	// 		P *int64
	// 	}{
	// 		M: "m",
	// 		N: sql.NullString{Valid: true, String: "n"},
	// 		P: new(int64),
	// 	},
	// }))
	// is.NoErr(err)

	v := 2
	ar, err := app.SubmitTask(ctx,
		task.NewTask(
			task.NewTaskOption(3, 2*time.Second).WithResultExpired(2*time.Minute),
			"testE",
			&v))
	is.NoErr(err)
	is.NotNil(ar)
	res, ok, err := ar.Wait(context.Background())
	is.NoErr(err)
	is.Equal(len(res), 1)
	is.True(ok)
	val, ok := res[0].(SimpleStruct)
	is.True(ok)
	is.NotNil(val)
	is.Equal(val.A, "result_A")
	is.Equal(val.B, 0)
	is.Equal(val.C.M, "result_M")
	is.NotNil(val.C.P)
	is.Equal(*val.C.P, v)
	fmt.Printf("testE result: %v, %v, %v\n", val, ok, err)

	time.Sleep(1500 * time.Second)
}
