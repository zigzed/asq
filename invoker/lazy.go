package invoker

import (
	"fmt"
	"reflect"
)

/*
 A less generic Invoker that can handle pointer with different level.
 This Invoker can only work with GobMarshaller and JsonSafeMarshaller.
*/
type lazyInvoker struct{}

func NewLazyInvoker() *lazyInvoker {
	return &lazyInvoker{}
}

func (vk lazyInvoker) Invoke(f interface{}, param []interface{}) ([]interface{}, error) {
	var (
		funcT = reflect.TypeOf(f)
		v     reflect.Value
		t     reflect.Type
	)

	// make sure parameter matched
	if len(param) != funcT.NumIn() {
		return nil, fmt.Errorf("parameter Count mismatch: %v %v", len(param), funcT.NumIn())
	}

	// compose input parameter
	var in = make([]reflect.Value, funcT.NumIn())
	for i := 0; i < funcT.NumIn(); i++ {
		v = reflect.ValueOf(param[i])

		// special handle for pointer
		if t = funcT.In(i); t.Kind() == reflect.Ptr {
			in[i] = *vk.toPointer(t, v)
		} else {
			in[i] = v
		}
	}

	// invoke the function
	ret := reflect.ValueOf(f).Call(in)
	out := make([]interface{}, 0, len(ret))

	for k, v := range ret {
		if v.CanInterface() {
			out = append(out, v.Interface())
		} else {
			return nil, fmt.Errorf("unable to convert to interface{} for %d, %v", k, v)
		}
	}

	return out, nil
}

func (vk lazyInvoker) Return(f interface{}, returns []interface{}) ([]interface{}, error) {
	var (
		funcT = reflect.TypeOf(f)
		t     reflect.Type
		r     reflect.Value
	)
	if len(returns) != funcT.NumOut() {
		return nil, fmt.Errorf("parameter Count mismatch: %v %v", len(returns), funcT.NumOut())
	}

	for k, v := range returns {
		t = funcT.Out(k)
		// special handle for pointer
		if t.Kind() == reflect.Ptr {
			if r = *vk.toPointer(t, reflect.ValueOf(v)); r.CanInterface() {
				returns[k] = r.Interface()
			} else {
				return nil, fmt.Errorf("can't interface of %v from %d:%v", r, k, v)
			}
		}
	}

	return returns, nil
}

func (vk lazyInvoker) toPointer(t reflect.Type, v reflect.Value) *reflect.Value {
	if t.Kind() != reflect.Ptr {
		return &v
	}

	vO := reflect.New(t)
	rO := vO.Elem()
	if v.IsValid() {
		for t.Kind() == reflect.Ptr {
			if t = t.Elem(); t.Kind() == reflect.Ptr {
				vO.Elem().Set(reflect.New(t))
				vO = vO.Elem()
			} else {
				if v.Kind() != reflect.Ptr {
					vO.Elem().Set(reflect.New(t))
					vO = vO.Elem()
				}
			}
		}
		for v.Kind() == reflect.Ptr {
			if v.Elem().Kind() == reflect.Ptr {
				v = v.Elem()
			} else {
				break
			}
		}
		vO.Elem().Set(v)
	}

	return &rO
}
