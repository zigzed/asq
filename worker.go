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
	tick := time.NewTicker(500 * time.Millisecond)
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-tick.C:
			for _, name := range w.fnMgr.registered() {
				task, err := w.broker.Poll(ctx, name)
				if err != nil {
					w.logger.Errorf("polling for task %s failed: %v", name, err)
					continue
				}
				if task != nil {
					if err := w.execute(ctx, task); err != nil {
						w.logger.Errorf("execute task %s for %v failed: %v", name, task, err)
					}
				}
			}
		}
	}

	tick.Stop()
	return nil
}

func (w *Worker) execute(ctx context.Context, task *task.Task) error {
	fn, err := w.fnMgr.lookup(task.Name)
	if err != nil {
		return errors.Wrapf(err, "function %s not found", task.Name)
	}

	returns, err := w.invoker.Invoke(fn, task.Args)
	if err != nil {
		return errors.Wrapf(err, "execute %v failed", task)
	}

	lastValue, returns := returns[len(returns)-1], returns[:len(returns)-1]
	// 函数执行没有返回错误
	if lastValue == nil {
		if len(task.OnSuccess) == 0 {
			if !task.Option.IgnoreResult {
				return w.backend.Push(ctx,
					result.NewResult(
						task.Id,
						task.Name,
						returns,
						time.Duration(task.Option.ResultExpired)*time.Second))
			}
			return nil
		}
		task = task.OnSuccess[0]
		if len(returns) > 0 {
			task.Args = append(task.Args, returns...)
		}
		return w.broker.Push(ctx, task)
	}

	task.Option.RetryCount -= 1
	if task.Option.RetryCount < 0 {
		return errors.Wrapf(err, "execute %v failed", task)
	}

	scheduleAt := time.Now().Add(time.Duration(task.Option.RetryTimeout) * time.Millisecond).UnixMilli()
	task.Option.StartAt = new(int64)
	*task.Option.StartAt = scheduleAt
	return w.broker.Push(ctx, task)
}
