package models

import (
	"reflect"
	"strings"
)

type Loader interface {
	Load(dataSourceName string) (*Schema, error)
}

type Schema struct {
	Tables []*Table
}

type Table struct {
	Name    string
	Columns Columns
	Indexes []*Index
}

type Columns []*Column

func (cs Columns) String() string {
	var s []string
	for _, c := range cs {
		s = append(s, c.Name)
	}
	return strings.Join(s, "And")
}

type Column struct {
	Name     string
	Type     reflect.Type
	Nullable bool
	Default  bool
}

type Index struct {
	Name    string
	Columns []*Column
	Unique  bool
}
