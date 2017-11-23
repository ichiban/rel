package sqlite3

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ichiban/rel/models"
)

const driverName = "sqlite3"

type Loader struct {
}

var _ models.Loader = (*Loader)(nil)

func (l *Loader) Load(dataSourceName string) (*models.Schema, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	var schema models.Schema

	var tableNames []string
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}

	for _, tableName := range tableNames {
		table := models.Table{
			Name: camel(tableName, true),
		}

		// check if it has rowid.
		_, err = db.Exec(fmt.Sprintf(`SELECT rowid FROM %s LIMIT 1`, tableName))
		rowid := err == nil

		var tableInfo []*TableInfo
		var pkColumns models.Columns
		rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var ti TableInfo
			if err := rows.Scan(&ti.CID, &ti.Name, &ti.Type, &ti.NotNull, &ti.DefaultValue, &ti.PK); err != nil {
				return nil, err
			}
			tableInfo = append(tableInfo, &ti)
			if ti.PK != 0 {
				pkColumns = append(pkColumns, nil)
			}
		}

		for _, column := range tableInfo {
			c := models.Column{
				Name:     camel(column.Name, true),
				Type:     parseType(column.Type),
				Nullable: !column.NotNull && !(rowid && column.PK == 1 && strings.EqualFold(column.Type, "INTEGER") && len(pkColumns) == 1),
				Default:  column.DefaultValue != nil,
			}
			table.Columns = append(table.Columns, &c)
			if column.PK > 0 {
				pkColumns[column.PK-1] = &c
			}
		}

		if len(pkColumns) != 0 {
			table.Indexes = []*models.Index{
				{Name: pkColumns.String(), Columns: pkColumns, Unique: true},
			}
		}

		var indexes []*Index
		rows, err = db.Query(fmt.Sprintf(`PRAGMA index_list(%s)`, table.Name))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var index Index
			if err := rows.Scan(&index.Seq, &index.Name, &index.Unique, &index.Origin, &index.Partial); err != nil {
				return nil, err
			}
			indexes = append(indexes, &index)
		}
		for _, index := range indexes {
			i := models.Index{
				Unique: index.Unique,
			}

			rows, err = db.Query(fmt.Sprintf(`PRAGMA index_info(%s)`, index.Name))
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var ii IndexInfo
				if err := rows.Scan(&ii.SeqNo, &ii.CID, &ii.Name); err != nil {
					return nil, err
				}
				i.Columns = append(i.Columns, table.Columns[ii.CID])
			}

			switch index.Origin {
			case "c": // CREATE INDEX
				i.Name = camel(index.Name, true)
			case "u": // UNIQUE CONSTRAINT
				i.Name = table.Columns.String()
			case "pk": // PRIMARY KEY CONSTRAINT
				i.Name = table.Columns.String()
			}

			table.Indexes = append(table.Indexes, &i)
		}

		schema.Tables = append(schema.Tables, &table)
	}

	return &schema, nil
}

type TableInfo struct {
	CID          int64
	Name         string
	Type         string
	NotNull      bool
	DefaultValue *string
	PK           int64
}

type Index struct {
	Seq     int64
	Name    string
	Unique  bool
	Origin  string
	Partial bool
}

type IndexInfo struct {
	SeqNo int64
	CID   int64
	Name  string
}

func parseType(t string) reflect.Type {
	t = strings.ToUpper(t)
	switch {
	case strings.Contains(t, "INT"):
		return reflect.TypeOf(int64(0))
	case strings.Contains(t, "CHAR"), strings.Contains(t, "CLOB"), strings.Contains(t, "TEXT"):
		return reflect.TypeOf("")
	case strings.Contains(t, "BLOB"), t == "":
		return reflect.TypeOf([]byte{})
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"):
		return reflect.TypeOf(float64(0))
	case t == "TIMESTAMP", t == "DATETIME", t == "DATE":
		return reflect.TypeOf(time.Time{})
	case t == "BOOLEAN":
		return reflect.TypeOf(false)
	default:
		return reflect.TypeOf(nil)
	}
}

var idPattern = regexp.MustCompile(`Id\z`)

func camel(s string, up bool) string {
	var result string
	for _, rune := range s {
		if unicode.IsPunct(rune) {
			up = true
			continue
		}

		if up {
			result += strings.ToUpper(string(rune))
			up = false
			continue
		}

		result += string(rune)
	}
	return idPattern.ReplaceAllString(result, "ID")
}
