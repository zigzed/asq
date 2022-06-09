package result

import "time"

type Result struct {
	Id      string
	Name    string
	Timeout time.Duration
	Results []interface{}
	Error   error
}

func NewResult(id, name string, results []interface{}, err error, timeout time.Duration) *Result {
	return &Result{
		Id:      id,
		Name:    name,
		Results: results,
		Error:   err,
		Timeout: timeout,
	}
}
