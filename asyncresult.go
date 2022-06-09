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
