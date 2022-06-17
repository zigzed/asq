package asq

import (
	"context"
	"time"

	"emperror.dev/errors"
	"github.com/zigzed/asq/invoker"
	"github.com/zigzed/asq/result"
	"github.com/zigzed/asq/task"
	"golang.org/x/sync/errgroup"
)

type Worker struct {
	broker  Broker
	backend Backend
	logger  Logger
	fnMgr   *fnManager
	invoker Invoker
}

func newWorker(broker Broker, backend Backend, mgr *fnManager, logger Logger) *Worker {
	w := &Worker{
		broker:  broker,
		backend: backend,
		logger:  logger,
		fnMgr:   mgr,
		invoker: invoker.NewGenericInvoker(),
	}
	return w
}

func (w *Worker) Start(ctx context.Context, size int) error {
	eg := new(errgroup.Group)
	for i := 0; i < size; i++ {
		eg.Go(func() error {
			return w.doPoll(ctx)
		})
	}

	return eg.Wait()
}

func (w *Worker) doPoll(ctx context.Context) error {
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			task, err := w.broker.Poll(ctx, time.Second, w.fnMgr.registered()...)
			if err != nil {
				w.logger.Errorf("polling for task %v failed: %v", w.fnMgr.registered(), err)
				continue
			}
			if task != nil {
				if err := w.execute(ctx, task); err != nil {
					w.logger.Errorf("execute task %s failed: %v", w.fnMgr.registered(), err)
				}
			}
		}
	}

	return nil
}

func (w *Worker) execute(ctx context.Context, task *task.Task) error {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Errorf("panic: execute task %+v failed: %v", task, r)
		}
	}()

	fn, err := w.fnMgr.lookup(task.Name)
	if err != nil {
		return errors.Wrapf(err, "function %s not found", task.Name)
	}

	var lastError interface{}
	invoke := func(p Invoker, fn interface{}, args []interface{}) ([]interface{}, error) {
		defer func() {
			if r := recover(); r != nil {
				lastError = errors.Errorf("panic: invoke %+v failed: %v", task, r)
				w.logger.Errorf("panic: invoke %+v failed: %v", task, r)
			}
		}()
		return p.Invoke(fn, args)
	}

	returns, err := invoke(w.invoker, fn, task.Args)
	if err != nil {
		return errors.Wrapf(err, "execute %+v failed", task)
	}

	if len(returns) > 0 {
		lastError, returns = returns[len(returns)-1], returns[:len(returns)-1]
		// 函数执行没有返回错误
		if lastError == nil {
			if len(task.OnSuccess) == 0 {
				if !task.Option.IgnoreResult {
					return w.backend.Push(ctx,
						result.NewResult(
							task.Id,
							task.Name,
							returns,
							nil,
							time.Duration(task.Option.ResultExpired)*time.Second))
				}
				return nil
			}
			task = task.OnSuccess[0]
			if len(returns) >= 1 {
				task.Args = append(task.Args, returns...)
			}
			return w.broker.Push(ctx, task)
		}
	}

	if task.Option.RetryCount <= task.BackOff.Attempts {
		err, _ := lastError.(error)
		return w.backend.Push(ctx,
			result.NewResult(
				task.Id,
				task.Name,
				returns,
				err,
				time.Duration(task.Option.ResultExpired)*time.Second))
	}

	nextAttempt := task.BackOff.NextAttempt()
	w.logger.Errorf("task %s, %s failed: %v, next attempt in %.3f seconds",
		task.Name, task.Id, lastError, nextAttempt.Seconds())

	scheduleAt := time.Now().Add(nextAttempt).UnixMilli()
	task.Option.StartAt = new(int64)
	*task.Option.StartAt = scheduleAt
	return w.broker.Push(ctx, task)
}
