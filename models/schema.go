package models

import (
	"bytes"
	"go/format"
	"io"
	"log"
	"reflect"
	"text/template"
)

var Templates *template.Template

type Schema struct {
	Driver  string
	Package string
	Tables  []Table
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

func (s *Schema) WriteTo(w io.Writer) (int64, error) {
	var buf bytes.Buffer
	if err := Templates.ExecuteTemplate(&buf, "/rel.tmpl", &s); err != nil {
		log.Printf("failed to execute template: %v", err)
		return 0, err
	}

	b, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("failed to format: %v", err)
		return 0, err
	}

	n, err := w.Write(b)
	if err != nil {
		log.Printf("failed to write: %v", err)
		return 0, err
	}
	return int64(n), nil
}
