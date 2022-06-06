package result

import "time"

type Result struct {
	Id      string
	Name    string
	Timeout time.Duration
	Results []interface{}
}

func NewResult(id, name string, results []interface{}, timeout time.Duration) *Result {
	return &Result{
		Id:      id,
		Name:    name,
		Results: results,
		Timeout: timeout,
	}
}
