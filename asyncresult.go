package asq

import (
	"context"
	"reflect"
	"time"

	"github.com/zigzed/asq/invoker"
)

type AsyncResult struct {
	backend      Backend
	id           string
	name         string
	ignoreResult bool
	pollInterval time.Duration
}

func (ar *AsyncResult) Wait(ctx context.Context, args ...interface{}) (bool, error) {
	if ar.ignoreResult {
		return true, nil
	}

	tick := time.NewTicker(ar.pollInterval)

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

func (ar *AsyncResult) Then(ctx context.Context, onSuccess interface{}, onFailed interface{}) {
	if ar.ignoreResult {
		return
	}

	funcT := reflect.TypeOf(onSuccess)
	param := make([]interface{}, funcT.NumIn())
	for i := 0; i < funcT.NumIn(); i++ {
		param[i] = reflect.New(funcT.In(i)).Interface()
	}

	go func() {
		ok, err := ar.Wait(ctx, param...)
		if ok && err == nil && onSuccess != nil {
			args := make([]interface{}, len(param))
			for i := 0; i < len(param); i++ {
				args[i] = reflect.Indirect(reflect.ValueOf(param[i])).Interface()
			}
			invoker.NewGenericInvoker().Invoke(onSuccess, args)
		}
		if ok && err != nil && onFailed != nil {
			invoker.NewGenericInvoker().Invoke(onFailed, []interface{}{err})
		}
	}()
}
