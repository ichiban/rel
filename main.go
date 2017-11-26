package main

import (
	"database/sql"
	"fmt"
	"html/template"
	"os"

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

	fmt.Printf("name: %s\n", dataSourceName)

	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		panic(err)
	}

	l := sqlite3.Loader{DB: db}
	var s models.Schema
	if err := l.Load(&s); err != nil {
		panic(err)
	}

	s.Package = "main"
	ts := template.Must(template.New("").Funcs(template.FuncMap{
		"singular": inflect.Singularize,
		"Camel":    snaker.SnakeToCamel,
		"camel":    inflect.CamelizeDownFirst,
	}).ParseGlob("templates/*.tmpl"))
	if err := ts.ExecuteTemplate(os.Stdout, "model.tmpl", &s); err != nil {
		panic(err)
	}
}
