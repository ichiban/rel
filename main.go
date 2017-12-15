package main

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/ichiban/rel/models"
	"github.com/ichiban/rel/postgres"
	"github.com/ichiban/rel/sqlite3"
)

var (
	driver      = flag.String("driver", "", "-driver postgres")
	database    = flag.String("database", "", "-database postgres://foo:bar@localhost/baz?sslmode=disable")
	packageName = flag.String("package", "", "-package models")
)

func init() {
	flag.Parse()
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

	s.Driver = *driver
	s.Package = filepath.Base(*packageName)
	if _, err := s.WriteTo(os.Stdout); err != nil {
		log.Fatalf("failed to write: %v", err)
	}
}
