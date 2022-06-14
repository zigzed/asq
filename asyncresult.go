package asq

import (
	"context"
	"time"
)

type AsyncResult struct {
	backend Backend
	id      string
	name    string
}

func (ar *AsyncResult) Wait(ctx context.Context, interval time.Duration, args ...interface{}) (bool, error) {
	tick := time.NewTicker(interval)

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-tick.C:
			ok, err := ar.backend.Scan(ctx, ar.id, ar.name, args...)
			if !ok {
				continue
			}
			return true, err
		}
	}

	tick.Stop()

	return false, nil
}

// func (ar *AsyncResult) Then(ctx context.Context, interval time.Duration, fn interface{}) {
// 	funcT := reflect.TypeOf(fn)
// 	param := make([]interface{}, funcT.NumIn())
// 	for i := 0; i < funcT.NumIn(); i++ {
// 		param[i] = reflect.New(funcT.In(i)).Interface()
// 	}

// 	go func() {
// 		ok, err := ar.Wait(ctx, interval, param...)
// 		if ok && err == nil {
// 			invoker.NewGenericInvoker().Invoke(fn, param)
// 		}
// 	}()
// }
