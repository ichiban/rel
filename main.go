package main

import (
	"fmt"
	"os"

	"github.com/ichiban/rel/sqlite3"
)

func main() {
	if len(os.Args) < 2 {
		panic(os.Args)
	}

	dataSourceName := os.Args[1]

	fmt.Printf("name: %s\n", dataSourceName)

	var l sqlite3.Loader
	s, err := l.Load(dataSourceName)
	if err != nil {
		panic(err)
	}

	for _, t := range s.Tables {
		fmt.Printf("table: %s\n", t.Name)
		for _, c := range t.Columns {
			fmt.Printf("column: %s, type=%s, nullable=%t\n", c.Name, c.Type.Name(), c.Nullable)
		}
		for _, i := range t.Indexes {
			fmt.Printf("index: %s, unique=%t\n", i.Name, i.Unique)
			for _, c := range i.Columns {
				fmt.Printf("column: %s, type=%s, nullable=%t\n", c.Name, c.Type.Name(), c.Nullable)
			}
		}
	}
}
