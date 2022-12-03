package asq

import (
	"context"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/zigzed/asq/invoker"
	"github.com/zigzed/asq/result"
	"github.com/zigzed/asq/task"
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

func (w *Worker) Start(ctx context.Context, size int) {
	tasks := make(chan *task.Task, size)
	defer func() {
		close(tasks)
	}()

	go func() {
		w.doPoll(ctx, tasks)
	}()

	var wg sync.WaitGroup
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func() {
			w.doExecute(ctx, tasks)
			wg.Done()
		}()
	}

	wg.Wait()
}

func (w *Worker) doPoll(ctx context.Context, tasks chan<- *task.Task) {
	w.logger.Infof("asq: polling task %v is starting...", w.fnMgr.registered())
	defer w.logger.Infof("asq: polling task %v is stopped...", w.fnMgr.registered())

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			task, err := w.broker.Poll(ctx, 30*time.Second)
			if err != nil && !errors.Is(err, context.Canceled) {
				w.logger.Errorf("polling for task %v failed: %v", w.fnMgr.registered(), err)
				continue
			}
			if task != nil {
				tasks <- task
			}
		}
	}
}

func (w *Worker) doExecute(ctx context.Context, tasks <-chan *task.Task) {
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-tasks:
			if !ok {
				return
			}
			if err := w.execute(ctx, task); err != nil {
				w.logger.Errorf("execute task %s with %v failed: %v", task.Name, task.Args, err)
			}
		}
	}
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
		rer := w.backend.Push(ctx,
			result.NewResult(
				task.Id,
				task.Name,
				returns,
				err,
				time.Duration(task.Option.ResultExpired)*time.Second))
		w.logger.Errorf("task %s, %s failed: %v", task.Name, task.Id, err)
		return rer
	}

	nextAttempt := task.BackOff.NextAttempt()
	w.logger.Errorf("task %s, %s failed: %v, next attempt in %.3f seconds",
		task.Name, task.Id, lastError, nextAttempt.Seconds())

	scheduleAt := time.Now().Add(nextAttempt).UnixMilli()
	task.Option.StartAt = new(int64)
	*task.Option.StartAt = scheduleAt
	return w.broker.Push(ctx, task)
}
