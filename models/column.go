package models

import (
	"reflect"
)

type Column struct {
	Name     string
	RawType  reflect.Type
	Nullable bool
	Default  bool
}

func (c *Column) Type() reflect.Type {
	t := c.RawType
	if c.Nullable {
		t = reflect.PtrTo(t)
	}
	return t
}
