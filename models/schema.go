package models

import (
	"reflect"
)

type Schema struct {
	Dialect string
	Package string
	Tables  []*Table
}

func (s *Schema) Columns() []Column {
	m := map[Column]struct{}{}
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			g := Column{
				Name:    c.Name,
				RawType: reflect.TypeOf((interface{})(nil)),
			}
			m[g] = struct{}{}
		}
	}
	cs := make([]Column, 0, len(m))
	for c := range m {
		cs = append(cs, c)
	}
	return cs
}
