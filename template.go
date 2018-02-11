package rel

import (
	"io/ioutil"
	"log"
	"text/template"

	"reflect"

	"github.com/c9s/inflect"
	"github.com/knq/snaker"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

//go:generate go-assets-builder -s="templates" -o assets.go templates

var Templates *template.Template

func init() {
	Templates = template.New("/rel.tmpl")
	fs := template.FuncMap{
		"singular": inflect.Singularize,
		"plural":   inflect.Pluralize,
		"Camel":    snaker.SnakeToCamel,
		"camel":    inflect.CamelizeDownFirst,
		"zero":     zero,
	}
	for n, f := range Assets.Files {
		b, err := ioutil.ReadAll(f)
		if err != nil {
			log.Fatalf("failed to read template: %v", err)
		}
		t, err := Templates.New("").Funcs(fs).Parse(string(b))
		if err != nil {
			log.Fatalf("failed to parse template: %v", err)
		}
		Templates, err = Templates.AddParseTree(n, t.Tree)
		if err != nil {
			log.Fatalf("failed to add template: %v", err)
		}
	}
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
