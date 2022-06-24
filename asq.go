package asq

import (
	"context"

	"emperror.dev/errors"
	"github.com/google/uuid"
	"github.com/zigzed/asq/redis"
	"github.com/zigzed/asq/task"
)

type App struct {
	mgr     *fnManager
	broker  Broker
	backend Backend
	logger  Logger
}

type Options func(*App)

func WithLogger(logger Logger) Options {
	return func(app *App) {
		app.logger = logger
	}
}

func NewApp(broker Broker, backend Backend, opts ...Options) *App {
	app := &App{
		mgr:     newFnManager(),
		broker:  broker,
		backend: backend,
		logger:  defaultLogger{},
	}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

func NewAppFromRedis(cfg redis.Option, queue string, opts ...Options) (*App, error) {
	broker, err := redis.NewBroker(&cfg, queue)
	if err != nil {
		return nil, err
	}
	backend, err := redis.NewBackend(&cfg, queue)
	if err != nil {
		return nil, err
	}

	return NewApp(broker, backend, opts...), nil
}

func (app *App) Register(name string, fn interface{}) error {
	return app.mgr.register(name, fn)
}

func (app *App) StartWorker(ctx context.Context, size int) error {
	worker := newWorker(app.broker, app.backend, app.mgr, app.logger)
	return worker.Start(ctx, size)
}

func (app *App) SubmitTask(ctx context.Context, tasks ...*task.Task) (*AsyncResult, error) {
	for _, task := range tasks {
		if task.Id == "" {
			task.Id = uuid.New().String()
		}
	}

	task := app.makeTaskLink(tasks...)
	if err := app.broker.Push(ctx, task); err != nil {
		return nil, errors.Wrapf(err, "push task %v failed", tasks)
	}

	if len(tasks) > 0 {
		task := tasks[len(tasks)-1]
		return &AsyncResult{
			backend:      app.backend,
			id:           task.Id,
			name:         task.Name,
			ignoreResult: task.Option.IgnoreResult,
		}, nil
	}

	return nil, nil
}

func (app *App) makeTaskLink(tasks ...*task.Task) *task.Task {
	for i := len(tasks) - 1; i > 0; i-- {
		if i > 0 {
			tasks[i-1].OnSuccess = []*task.Task{tasks[i]}
		}
	}
	return tasks[0]
}
