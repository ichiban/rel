package sqlite3

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ichiban/rel"
)

type Loader struct {
	DB *sql.DB
}

var _ rel.Loader = (*Loader)(nil)

func (l *Loader) Load(schema *rel.Schema) error {
	db := l.DB

	var tableNames []string
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		tableNames = append(tableNames, name)
	}

	for _, tableName := range tableNames {
		table := rel.Table{
			Name: tableName,
		}

		// check if it has rowid.
		if _, err := db.Query(fmt.Sprintf(`SELECT rowid FROM %s LIMIT 1`, tableName)); err != nil {
			return fmt.Errorf("WITHOUT ROWID tables are not supported: %v", err)
		}

		var tableInfo []*TableInfo
		var pkColumns int
		rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
		if err != nil {
			return err
		}
		for rows.Next() {
			var ti TableInfo
			if err := rows.Scan(&ti.CID, &ti.Name, &ti.Type, &ti.NotNull, &ti.DefaultValue, &ti.PK); err != nil {
				return err
			}
			tableInfo = append(tableInfo, &ti)
			if ti.PK != 0 {
				pkColumns++
			}
		}

		table.Columns = make([]rel.Column, 0, len(tableInfo))
		table.PrimaryKey = make([]rel.Column, pkColumns)

		for _, column := range tableInfo {
			rowidAlias := column.PK == 1 && strings.EqualFold(column.Type, "INTEGER") && pkColumns == 1
			c := rel.Column{
				Name:     column.Name,
				RawType:  parseType(column.Type),
				Nullable: !column.NotNull && !rowidAlias,
				Default:  column.DefaultValue != nil || rowidAlias,
			}
			table.Columns = append(table.Columns, c)
			if column.PK > 0 {
				table.PrimaryKey[column.PK-1] = c
			}
		}

		schema.Tables = append(schema.Tables, table)
	}

	return nil
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
		var i *interface{}
		return reflect.TypeOf(i).Elem()
	}
}
