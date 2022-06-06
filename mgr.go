package asq

import (
	"sync"

	"emperror.dev/errors"
)

type fnManager struct {
	sync.RWMutex
	fn map[string]interface{}
}

func newFnManager() *fnManager {
	return &fnManager{
		fn: make(map[string]interface{}, 0),
	}
}

func (fm *fnManager) register(name string, fn interface{}) error {
	fm.Lock()
	defer fm.Unlock()

	if _, ok := fm.fn[name]; ok {
		return errors.Errorf("function %s registered", name)
	}

	fm.fn[name] = fn
	return nil
}

func (fm *fnManager) lookup(name string) (interface{}, error) {
	fm.RLock()
	defer fm.RUnlock()

	if fn, ok := fm.fn[name]; ok {
		return fn, nil
	}
	return nil, errors.Errorf("function %s not registered", name)
}

func (fm *fnManager) registered() []string {
	fm.RLock()
	defer fm.RUnlock()

	fn := make([]string, 0, len(fm.fn))
	for k := range fm.fn {
		fn = append(fn, k)
	}

	return fn
}
