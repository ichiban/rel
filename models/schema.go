package models

import (
	"reflect"
)

type Schema struct {
	Package string
	Tables  []*Table
}

func (s *Schema) Columns() []Column {
	var cs []Column
	m := map[Column]struct{}{}
	for _, t := range s.Tables {
		for _, c := range t.Columns {
			g := Column{
				Name:    c.Name,
				RawType: reflect.TypeOf((interface{})(nil)),
			}
			if _, ok := m[g]; ok {
				continue
			}
			cs = append(cs, g)
			m[g] = struct{}{}
		}
	}
	return cs
}
