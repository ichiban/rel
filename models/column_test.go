package models

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumn_Type(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		column Column
		result reflect.Type
	}{
		{
			column: Column{
				Name:     "foo",
				RawType:  reflect.TypeOf(int64(0)),
				Nullable: false,
			},
			result: reflect.TypeOf(int64(0)),
		},
		{
			column: Column{
				Name:     "foo",
				RawType:  reflect.TypeOf(int64(0)),
				Nullable: true,
			},
			result: reflect.PtrTo(reflect.TypeOf(int64(0))),
		},
	}

	for _, tc := range testCases {
		assert.Equal(tc.result, tc.column.Type())
	}
}
