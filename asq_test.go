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

type simpleStruct struct {
	A string
	B int
	C struct {
		M string
		N sql.NullString
		P *int
	}
}

func testD(v simpleStruct) error {
	fmt.Printf("  in testD(%+v) done\n", v)
	return nil
}

var testEv = 5

func testE(v *int) (*simpleStruct, error) {
	testEv--
	r := &simpleStruct{
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
	}
	if testEv > 0 {
		fmt.Printf("  inTestE(%v) error\n", testEv)
		return r, errors.Errorf("test error %d", testEv)
	}

	fmt.Printf("  inTestE(%v) done\n", testEv)
	return r, nil
}

func testF() error {
	panic("  testF panic")
	return nil
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
	err = app.Register("testF", testF)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err = app.StartWorker(ctx, 2)
		is.NoErr(err)
	}()

	_, err = app.SubmitTask(ctx, task.NewTask(nil, "testA"))
	is.NoErr(err)

	_, err = app.SubmitTask(ctx, task.NewTask(nil, "testB", 3))
	is.NoErr(err)

	ar, err := app.SubmitTask(ctx, task.NewTask(nil, "testC", 30))
	is.NoErr(err)
	is.NotNil(ar)
	var (
		valc int
		errc error
	)
	ok, err := ar.Wait(context.Background(), 500*time.Millisecond, &valc)
	is.True(ok)
	is.Equal(valc, 60)
	is.NoErr(errc)
	fmt.Printf("testC result: %+v, %+v\n", valc, errc)

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

	testEv = 5
	v := 2
	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			task.NewTaskOption(3, 2*time.Second).WithResultExpired(2*time.Minute),
			"testE",
			&v))
	is.NoErr(err)
	is.NotNil(ar)

	var (
		vale simpleStruct
	)
	ok, err = ar.Wait(context.Background(), 500*time.Millisecond, &vale)
	is.True(ok)
	is.Err(err)
	fmt.Printf("testE result: %v, %v, %v\n", vale, ok, err)

	testEv = 2
	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			task.NewTaskOption(3, 2*time.Second).WithResultExpired(2*time.Minute),
			"testE",
			&v))
	ok, err = ar.Wait(context.Background(), 500*time.Millisecond, &vale)
	is.NoErr(err)
	is.True(ok)
	is.Equal(vale.A, "result_A")
	is.Equal(vale.B, 0)
	is.Equal(vale.C.M, "result_M")
	is.NotNil(vale.C.P)
	is.Equal(*vale.C.P, v)
	fmt.Printf("testE result: %v, %v, %v\n", vale, ok, err)

	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			nil,
			"testF"))
	ok, err = ar.Wait(context.Background(), 500*time.Millisecond)
	fmt.Printf("testF result: %v, %v\n", ok, err)

	_, err = app.SubmitTask(ctx,
		task.NewTask(
			nil,
			"testA"))
	is.NoErr(err)

	time.Sleep(1500 * time.Second)
}
