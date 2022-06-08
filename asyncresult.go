package asq

import (
	"context"
	"time"
)

type AsyncResult struct {
	backend      Backend
	pollInterval time.Duration
	id           string
	name         string
}

func (ar *AsyncResult) Wait(ctx context.Context) ([]interface{}, bool, error) {
	tick := time.NewTicker(ar.pollInterval)

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case <-tick.C:
			r, ok, err := ar.backend.Poll(ctx, ar.id, ar.name)
			if err != nil {
				continue
			}
			if !ok {
				continue
			}
			return r, true, nil
		}
	}

	tick.Stop()

	return nil, false, nil
}
