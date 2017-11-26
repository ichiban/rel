package models

import (
	"reflect"
)

type Column struct {
	Name     string
	Type     reflect.Type
	Nullable bool
	Default  bool
}
