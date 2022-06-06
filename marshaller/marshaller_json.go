package marshaller

import (
	"encoding/json"

	"emperror.dev/errors"
	"github.com/zigzed/asq/task"
)

type JsonMarshaller struct{}

func NewJsonMarshaller() *JsonMarshaller {
	return &JsonMarshaller{}
}

func (jm JsonMarshaller) EncodeTask(task *task.Task) (string, error) {
	if task == nil {
		return "", errors.Errorf("nil is not acceptable")
	}

	buf, err := json.Marshal(task)
	if err != nil {
		return "", errors.Wrapf(err, "json marshal for task %v failed", task)
	}
	return string(buf), nil
}

func (jm JsonMarshaller) DecodeTask(buf string) (*task.Task, error) {
	var task task.Task
	if err := json.Unmarshal([]byte(buf), &task); err != nil {
		return nil, errors.Wrapf(err, "json unmarshal for %s failed", buf)
	} else {
		return &task, nil
	}
}

func (jm JsonMarshaller) EncodeResult(rs []interface{}) (string, error) {
	buf, err := json.Marshal(rs)
	if err != nil {
		return "", errors.Wrapf(err, "json marshal for result %v failed", rs)
	}
	return string(buf), nil
}

func (jm JsonMarshaller) DecodeResult(buf string) ([]interface{}, error) {
	var rs []interface{}
	if err := json.Unmarshal([]byte(buf), &rs); err != nil {
		return nil, errors.Wrapf(err, "json unmarshal for %s failed", buf)
	} else {
		return rs, nil
	}
}
