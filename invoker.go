package asq

type Invoker interface {
	Invoke(interface{}, []interface{}) ([]interface{}, error)
	Return(interface{}, []interface{}) ([]interface{}, error)
}
