package marshaller

import "github.com/zigzed/asq/task"

type Marshaller interface {
	EncodeTask(task *task.Task) (string, error)
	DecodeTask(buf string) (*task.Task, error)
	EncodeResult(rs []interface{}) (string, error)
	DecodeResult(buf string) ([]interface{}, error)
}
