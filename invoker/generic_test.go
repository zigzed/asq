package invoker

import (
	"encoding/json"
	"fmt"
	"testing"

	"emperror.dev/errors"
	"github.com/cheekybits/is"
)

type testStruct1 struct {
	A string
	B int
	X *int
	C struct {
		M string
		N int
	}
}

func testFunc1(i testStruct1) (string, error) {
	fmt.Printf("testFunc: %v\n", i)
	return "test string result", errors.Errorf("test error marshal")
}

func TestInvoker1(t *testing.T) {
	is := is.New(t)

	inputs := `
[
	{
		"A": "a",
		"B": 1,
		"C": {
			"M": "m",
			"N": 666
		},
		"X": null
	}
]
	`

	vk := NewGenericInvoker()

	var args []interface{}
	err := json.Unmarshal([]byte(inputs), &args)
	is.NoErr(err)

	rs, err := vk.Invoke(testFunc1, args)
	is.NoErr(err)

	rv, err := vk.Return(testFunc1, rs)
	is.NoErr(err)

	output, err := json.Marshal(rv)
	is.NoErr(err)
	fmt.Printf("output: %v\n", string(output))
}

func TestAddElement(t *testing.T) {
	is := is.New(t)

	inputs := `
[
	{
		"A": "a",
		"B": 1,
		"C": {
			"M": "m",
			"N": 666
		},
		"X": null,
		"D": 123 
	}
]
	`

	vk := NewGenericInvoker()

	var args []interface{}
	err := json.Unmarshal([]byte(inputs), &args)
	is.NoErr(err)

	rs, err := vk.Invoke(testFunc1, args)
	is.NoErr(err)

	rv, err := vk.Return(testFunc1, rs)
	is.NoErr(err)

	output, err := json.Marshal(rv)
	is.NoErr(err)
	fmt.Printf("output: %v\n", string(output))
}
