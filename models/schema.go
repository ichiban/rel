package models

import (
	"bytes"
	"go/format"
	"io"
	"log"
	"reflect"
	"text/template"

	"github.com/c9s/inflect"
	"github.com/knq/snaker"
)

type Schema struct {
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
	ts := template.Must(template.New("").Funcs(template.FuncMap{
		"singular": inflect.Singularize,
		"plural":   inflect.Pluralize,
		"Camel":    snaker.SnakeToCamel,
		"camel":    inflect.CamelizeDownFirst,
		"zero":     zero,
	}).ParseGlob("templates/*.tmpl"))

	var buf bytes.Buffer
	if err := ts.ExecuteTemplate(&buf, "rel.tmpl", &s); err != nil {
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

func zero(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "false"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return "0"
	case reflect.Array, reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return "nil"
	case reflect.String:
		return `""`
	case reflect.Struct:
		return "(" + t.String() + "{})"
	default:
		log.Fatalf("unsupported type: %s", t)
		return ""
	}
}
