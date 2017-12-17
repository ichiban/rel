package main

import (
	"database/sql"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"text/template"

	"github.com/c9s/inflect"
	"github.com/knq/snaker"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/ichiban/rel/models"
	"github.com/ichiban/rel/postgres"
	"github.com/ichiban/rel/sqlite3"
)

//go:generate go-assets-builder -s="/templates" -o assets.go templates

var (
	driver      = flag.String("driver", "", "-driver postgres")
	database    = flag.String("database", "", "-database postgres://foo:bar@localhost/baz?sslmode=disable")
	packageName = flag.String("package", "", "-package models")
	path        = flag.String("path", "", "-path ./models/rel.go")
)

func init() {
	flag.Parse()
}

func init() {
	models.Templates = template.New("/rel.tmpl")
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
		t, err := models.Templates.New("").Funcs(fs).Parse(string(b))
		if err != nil {
			log.Fatalf("failed to parse template: %v", err)
		}
		models.Templates, err = models.Templates.AddParseTree(n, t.Tree)
		if err != nil {
			log.Fatalf("failed to add template: %v", err)
		}
	}
}

func main() {
	db, err := sql.Open(*driver, *database)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}

	var l models.Loader
	switch *driver {
	case "sqlite3":
		l = &sqlite3.Loader{DB: db}
	case "postgres":
		l = &postgres.Loader{DB: db}
	}

	var s models.Schema
	if err := l.Load(&s); err != nil {
		log.Fatalf("failed to load schema: %v", err)
	}

	out := os.Stdout
	if *path != "" && *path != "-" {
		out, err = os.OpenFile(*path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}
	}

	s.Driver = *driver
	s.Package = filepath.Base(*packageName)
	if _, err := s.WriteTo(out); err != nil {
		log.Fatalf("failed to write: %v", err)
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
