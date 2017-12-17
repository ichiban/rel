package postgres

import (
	"database/sql"
	"reflect"

	"time"

	"github.com/ichiban/rel/models"
)

type Loader struct {
	DB *sql.DB
}

var _ models.Loader = (*Loader)(nil)

func (l *Loader) Load(schema *models.Schema) error {
	db := l.DB

	var tableNames []string
	rows, err := db.Query(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`)
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
		table := models.Table{
			Name: tableName,
		}

		var columns []Columns
		rows, err := db.Query(`SELECT column_name, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1`, tableName)
		if err != nil {
			return err
		}
		for rows.Next() {
			var c Columns
			if err := rows.Scan(&c.ColumnName, &c.ColumnDefault, &c.IsNullable, &c.DataType); err != nil {
				return err
			}
			columns = append(columns, c)
		}

		table.Columns = make([]models.Column, 0, len(columns))
		for _, c := range columns {
			table.Columns = append(table.Columns, c.Column())
		}

		primaryKeys := make([]Columns, 0, len(columns))
		rows, err = db.Query(`
			SELECT c.column_name, c.column_default, c.is_nullable, c.data_type
			FROM information_schema.columns c
			INNER JOIN information_schema.key_column_usage kcu
			ON c.table_schema = kcu.table_schema
			AND c.table_name = kcu.table_name
			AND c.column_name = kcu.column_name
			INNER JOIN information_schema.table_constraints tc
			ON c.table_schema = tc.table_schema
			AND c.table_name = tc.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
			AND tc.constraint_name = kcu.constraint_name
			AND c.table_schema = 'public'
			AND c.table_name = $1
			ORDER BY kcu.ordinal_position
		`, tableName)
		if err != nil {
			return err
		}
		for rows.Next() {
			var c Columns
			if err := rows.Scan(&c.ColumnName, &c.ColumnDefault, &c.IsNullable, &c.DataType); err != nil {
				return err
			}
			primaryKeys = append(primaryKeys, c)
		}

		table.PrimaryKey = make([]models.Column, 0, len(primaryKeys))
		for _, c := range columns {
			table.PrimaryKey = append(table.PrimaryKey, c.Column())
		}

		schema.Tables = append(schema.Tables, table)
	}

	return nil
}

type Columns struct {
	ColumnName    string
	ColumnDefault *string
	IsNullable    string
	DataType      string
}

func (c *Columns) Column() models.Column {
	return models.Column{
		Name:     c.ColumnName,
		RawType:  parseType(c.DataType),
		Nullable: c.IsNullable == "YES",
		Default:  c.ColumnDefault != nil,
	}
}

func parseType(s string) reflect.Type {
	switch s {
	case "bigint":
		return reflect.TypeOf(int64(0))
	case "integer":
		return reflect.TypeOf(int32(0))
	case "smallint":
		return reflect.TypeOf(int16(0))
	case "character", "character varying", "text":
		return reflect.TypeOf("")
	case "boolean":
		return reflect.TypeOf(false)
	case "date", "time without time zone", "time with time zone", "timestamp without time zone", "timestamp with time zone":
		return reflect.TypeOf(time.Time{})
	case "bytea":
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(new(interface{})).Elem()
	}
}
