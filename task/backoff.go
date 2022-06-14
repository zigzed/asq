package task

import (
	"math"
	"time"
)

type BackOff struct {
	Attempts int
	Factor   float64
	Delay    time.Duration
}

func newBackOff(delay time.Duration, factor float64) *BackOff {
	return &BackOff{
		Attempts: 0,
		Factor:   factor,
		Delay:    delay,
	}
}

func (bo *BackOff) NextAttempt() time.Duration {
	next := bo.getNextAttempt(bo.Delay, bo.Factor, bo.Attempts)
	bo.Attempts++
	return next
}

func (bo *BackOff) getNextAttempt(delay time.Duration, factor float64, attempts int) time.Duration {
	d := time.Duration(delay.Seconds()*math.Pow(factor, float64(attempts+1))) * time.Second
	return bo.addJitter(d, delay)
}

func (bo *BackOff) addJitter(delay, unit time.Duration) time.Duration {
	return delay
}
