package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/ichiban/rel"
	"github.com/ichiban/rel/postgres"
	"github.com/ichiban/rel/sqlite3"
)

func main() {
	var driver string
	var database string
	var packageName string
	var path string
	var embed Embed

	flag.StringVar(&driver, "driver", "", "-driver postgres")
	flag.StringVar(&database, "database", "", "-database postgres://foo:bar@localhost/baz?sslmode=disable")
	flag.StringVar(&packageName, "package", "", "-package models")
	flag.StringVar(&path, "path", "", "-path ./models/rel.go")
	flag.Var(&embed, "embed", "-")
	flag.Parse()

	db, err := sql.Open(driver, database)
	if err != nil {
		log.Fatalf("failed to open DB: %v", err)
	}

	var l rel.Loader
	switch driver {
	case "sqlite3":
		l = &sqlite3.Loader{DB: db}
	case "postgres":
		l = &postgres.Loader{DB: db}
	}

	var s rel.Schema
	if err := l.Load(&s); err != nil {
		log.Fatalf("failed to load schema: %v", err)
	}

	out := os.Stdout
	if path != "" && path != "-" {
		out, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}
	}

	s.Driver = driver
	s.Package = filepath.Base(packageName)
	s.Embed = embed
	if _, err := s.WriteTo(out); err != nil {
		log.Fatalf("failed to write: %v", err)
	}
}

type Embed []string

func (e Embed) String() string {
	return strings.Join(e, "\n")
}

func (e *Embed) Set(s string) error {
	*e = append(*e, s)
	return nil
}
