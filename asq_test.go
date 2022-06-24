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
}

func testG(a, b, c int) (int, int, int, error) {
	fmt.Printf("  inTestG(%d, %d, %d) (%d, %d, %d)\n",
		a, b, c, a*2, b*2, c*2)
	return a * 2, b * 2, c * 2, nil
}

func testH() (time.Time, error) {
	ts := time.Now()
	fmt.Printf("  inTestH() %s", ts.Format("2006-01-02 15:04:05.000"))
	return ts, nil
}

func testI(a string, b int) (int, string, error) {
	fmt.Printf("  inTestI(%s, %d) (%d, %s)\n", a, b, b, a)
	return b, a, nil
}

func testJ() error {
	fmt.Printf("  inTestJ error\n")
	return errors.New("inTestJ err")
}

func testK() (time.Time, error) {
	now := time.Now()
	fmt.Printf("  inTestK time: %s\n", now.Format("2006-01-02 15:04:05.000000"))
	return now, nil
}

func testL() error {
	now := time.Now()
	fmt.Printf("  inTestK time: %s\n", now.Format("2006-01-02 15:04:05.000000"))
	return nil
}

func TestAsq(t *testing.T) {
	flag.Parse()

	is := is.New(t)
	app, err := NewAppFromRedis(redis.Option{
		Addrs: []string{"127.0.0.1:6379"},
	}, "test")
	is.NoErr(err)

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
	err = app.Register("testG", testG)
	is.NoErr(err)
	err = app.Register("testH", testH)
	is.NoErr(err)
	err = app.Register("testI", testI)
	is.NoErr(err)
	err = app.Register("testJ", testJ)
	is.NoErr(err)
	err = app.Register("testK", testK)
	is.NoErr(err)
	err = app.Register("testL", testL)
	is.NoErr(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err = app.StartWorker(ctx, 2)
		is.NoErr(err)
	}()

	_, err = app.SubmitTask(ctx, task.NewTask(nil, "testA"))
	is.NoErr(err)
	br()

	_, err = app.SubmitTask(ctx, task.NewTask(nil, "testB", 3))
	is.NoErr(err)
	br()

	ar, err := app.SubmitTask(ctx, task.NewTask(nil, "testC", 30))
	is.NoErr(err)
	is.NotNil(ar)
	var (
		valc int
		errc error
	)
	ok, err := ar.Wait(ctx, &valc)
	is.True(ok)
	is.Equal(valc, 60)
	is.NoErr(errc)
	fmt.Printf("testC result: %+v, %+v\n", valc, errc)
	br()

	_, err = app.SubmitTask(ctx, task.NewTask(nil, "testD", simpleStruct{
		A: "a",
		B: 1,
		C: struct {
			M string
			N sql.NullString
			P *int
		}{
			M: "m",
			N: sql.NullString{Valid: true, String: "n"},
			P: new(int),
		},
	}))
	is.NoErr(err)
	br()

	testEv = 5
	v := 2
	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			task.NewTaskOption(3, 2*time.Second).WithResultExpired(2*time.Minute),
			"testE",
			&v))
	is.NoErr(err)
	is.NotNil(ar)
	br()

	var (
		vale simpleStruct
	)
	ok, err = ar.Wait(ctx, &vale)
	is.True(ok)
	is.Err(err)
	fmt.Printf("testE result: %v, %v, %v\n", vale, ok, err)
	br()

	testEv = 2
	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			task.NewTaskOption(3, 2*time.Second).WithResultExpired(2*time.Minute),
			"testE",
			&v))
	ok, err = ar.Wait(ctx, &vale)
	is.NoErr(err)
	is.True(ok)
	is.Equal(vale.A, "result_A")
	is.Equal(vale.B, 0)
	is.Equal(vale.C.M, "result_M")
	is.NotNil(vale.C.P)
	is.Equal(*vale.C.P, v)
	fmt.Printf("testE result: %v, %v, %v\n", vale, ok, err)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(
			nil,
			"testF"))
	ok, err = ar.Wait(ctx)
	fmt.Printf("testF result: %v, %v\n", ok, err)
	br()

	_, err = app.SubmitTask(ctx,
		task.NewTask(
			nil,
			"testA"))
	is.NoErr(err)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testC", 300),
		task.NewTask(nil, "testB"))
	is.NoErr(err)
	ok, err = ar.Wait(ctx)
	is.NoErr(err)
	is.True(ok)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testC", 500),
		task.NewTask(nil, "testC"),
		task.NewTask(nil, "testC"))
	is.NoErr(err)
	ok, err = ar.Wait(ctx, &valc)
	is.NoErr(err)
	is.True(ok)
	is.Equal(valc, 4000)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testA"),
		task.NewTask(nil, "testC", 200),
		task.NewTask(nil, "testC"))
	is.NoErr(err)
	ok, err = ar.Wait(ctx, &valc)
	is.NoErr(err)
	is.True(ok)
	is.Equal(valc, 800)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testG", 1, 2, 3))
	is.NoErr(err)
	var x, y, z int
	ok, err = ar.Wait(ctx, &x, &y, &z)
	is.NoErr(err)
	is.True(ok)
	is.Equal(x, 2)
	is.Equal(y, 4)
	is.Equal(z, 6)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testG", 100, 200, 300),
		task.NewTask(nil, "testG"),
		task.NewTask(nil, "testG"))
	is.NoErr(err)
	ok, err = ar.Wait(ctx, &x, &y, &z)
	is.NoErr(err)
	is.True(ok)
	is.Equal(x, 800)
	is.Equal(y, 1600)
	is.Equal(z, 2400)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testH"))
	is.NoErr(err)
	var ts time.Time
	ok, err = ar.Wait(ctx, &ts)
	is.NoErr(err)
	is.True(ok)
	diff := time.Since(ts)
	fmt.Printf("testH timing: %.6f\n", diff.Seconds())
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testI", "mm", 111))
	is.NoErr(err)
	var m string
	var n int
	ok, err = ar.Wait(ctx, &n, &m)
	is.NoErr(err)
	is.True(ok)
	is.Equal(m, "mm")
	is.Equal(n, 111)
	br()

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testG", 3, 6, 9))
	is.NoErr(err)
	ar.Then(ctx,
		func(a, b, c int) {
			fmt.Printf("  inTestG::Then(%d, %d, %d)\n", a, b, c)
			is.Equal(a, 3*2)
			is.Equal(b, 6*2)
			is.Equal(c, 9*2)
			br()
		},
		func(err error) {
			fmt.Printf("error: %v\n", err)
		})

	ar, err = app.SubmitTask(ctx,
		task.NewTask(task.NewTaskOption(1, 1*time.Second), "testJ"))
	is.NoErr(err)
	ar.Then(ctx,
		func() {
			fmt.Printf("should not be called here\n")
			br()
		},
		func(err error) {
			fmt.Printf("then error: %v\n", err)
			br()
		})

	ar, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testK"))
	is.NoErr(err)
	ar.Then(ctx,
		func(called time.Time) {
			now := time.Now()
			dif := now.Sub(called)
			fmt.Printf("inTestK %s, timing: %.6f\n", now.Format("2006-01-02 15:04:05.000000"), dif.Seconds())
		},
		nil)

	_, err = app.SubmitTask(ctx,
		task.NewTask(nil, "testL"),
		task.NewTask(nil, "testL"),
		task.NewTask(nil, "testL"))
	is.NoErr(err)

	time.Sleep(3 * time.Second)
}

func br() {
	fmt.Printf("\n")
}
