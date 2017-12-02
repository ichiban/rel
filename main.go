package main

import (
	"database/sql"
	"log"
	"os"
	"reflect"
	"text/template"

	"github.com/c9s/inflect"
	_ "github.com/mattn/go-sqlite3"
	"github.com/serenize/snaker"

	"github.com/ichiban/rel/models"
	"github.com/ichiban/rel/sqlite3"
)

func main() {
	if len(os.Args) < 2 {
		panic(os.Args)
	}

	dataSourceName := os.Args[1]

	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		panic(err)
	}

	l := sqlite3.Loader{DB: db}
	var s models.Schema
	if err := l.Load(&s); err != nil {
		panic(err)
	}

	s.Dialect = "sqlite3"
	s.Package = "main"
	ts := template.Must(template.New("").Funcs(template.FuncMap{
		"singular": inflect.Singularize,
		"plural":   inflect.Pluralize,
		"Camel":    snaker.SnakeToCamel,
		"camel":    inflect.CamelizeDownFirst,
		"zero":     zero,
	}).ParseGlob("templates/*.tmpl"))
	if err := ts.ExecuteTemplate(os.Stdout, "model.tmpl", &s); err != nil {
		panic(err)
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
