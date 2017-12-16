package postgres

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"

	"github.com/ichiban/rel/models"
)

func TestLoader_Load(t *testing.T) {
	assert := assert.New(t)

	db, mock, err := sqlmock.New()
	assert.Nil(err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"table_name"})
	rows = rows.AddRow("accounts")
	rows = rows.AddRow("bottles")
	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"column_name", "column_default", "is_nullable", "data_type"})
	rows = rows.AddRow("id", columnDefault("nextval('accounts_id_seq'::regclass)"), "NO", "bigint")
	rows = rows.AddRow("name", columnDefault("''"), "NO", "text")
	rows = rows.AddRow("created_at", columnDefault("now()"), "NO", "timestamp with time zone")
	rows = rows.AddRow("updated_at", columnDefault("now()"), "NO", "timestamp with time zone")
	rows = rows.AddRow("created_by", columnDefault("''"), "NO", "text")
	mock.ExpectQuery(`SELECT column_name, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = \$1`).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"column_name", "column_default", "is_nullable", "data_type"})
	rows = rows.AddRow("id", nil, "NO", "bigint")
	mock.ExpectQuery(`SELECT c.column_name, c.column_default, c.is_nullable, c.data_type FROM information_schema.columns c INNER JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name INNER JOIN information_schema.table_constraints tc ON c.table_schema = tc.table_schema AND c.table_name = tc.table_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.constraint_name = kcu.constraint_name AND c.table_schema = 'public' AND c.table_name = \$1 ORDER BY kcu.ordinal_position`).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"column_name", "column_default", "is_nullable", "data_type"})
	rows = rows.AddRow("id", columnDefault("nextval('bottles_id_seq'::regclass)"), "NO", "bigint")
	rows = rows.AddRow("account_id", nil, "NO", "bigint")
	rows = rows.AddRow("rating", columnDefault("3"), "NO", "smallint")
	rows = rows.AddRow("name", columnDefault("''"), "NO", "text")
	rows = rows.AddRow("vineyard", columnDefault("''"), "NO", "text")
	rows = rows.AddRow("varietal", columnDefault("''"), "NO", "text")
	rows = rows.AddRow("vintage", columnDefault("1900"), "NO", "integer")
	rows = rows.AddRow("color", columnDefault("''"), "NO", "text")
	rows = rows.AddRow("sweetness", nil, "YES", "smallint")
	rows = rows.AddRow("country", nil, "YES", "text")
	rows = rows.AddRow("region", nil, "YES", "text")
	rows = rows.AddRow("review", nil, "YES", "text")
	rows = rows.AddRow("created_at", columnDefault("now()"), "NO", "timestamp with time zone")
	rows = rows.AddRow("updated_at", columnDefault("now()"), "NO", "timestamp with time zone")
	mock.ExpectQuery(`SELECT column_name, column_default, is_nullable, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = \$1`).WillReturnRows(rows)

	rows = sqlmock.NewRows([]string{"column_name", "column_default", "is_nullable", "data_type"})
	rows = rows.AddRow("id", nil, "NO", "bigint")
	mock.ExpectQuery(`SELECT c.column_name, c.column_default, c.is_nullable, c.data_type FROM information_schema.columns c INNER JOIN information_schema.key_column_usage kcu ON c.table_schema = kcu.table_schema AND c.table_name = kcu.table_name AND c.column_name = kcu.column_name INNER JOIN information_schema.table_constraints tc ON c.table_schema = tc.table_schema AND c.table_name = tc.table_name WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.constraint_name = kcu.constraint_name AND c.table_schema = 'public' AND c.table_name = \$1 ORDER BY kcu.ordinal_position`).WillReturnRows(rows)

	l := Loader{DB: db}
	var schema models.Schema
	assert.Nil(l.Load(&schema))
	assert.Nil(mock.ExpectationsWereMet())

	assert.Len(schema.Tables, 2)
	assert.Equal("accounts", schema.Tables[0].Name)
	assert.Len(schema.Tables[0].Columns, 5)
	assert.Equal("id", schema.Tables[0].Columns[0].Name)
	assert.Equal(reflect.TypeOf(int64(0)), schema.Tables[0].Columns[0].RawType)
	assert.False(schema.Tables[0].Columns[0].Nullable)
	assert.True(schema.Tables[0].Columns[0].Default)
	assert.Equal("name", schema.Tables[0].Columns[1].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[0].Columns[1].RawType)
	assert.False(schema.Tables[0].Columns[1].Nullable)
	assert.True(schema.Tables[0].Columns[1].Default)
	assert.Equal("created_at", schema.Tables[0].Columns[2].Name)
	assert.Equal(reflect.TypeOf(time.Time{}), schema.Tables[0].Columns[2].RawType)
	assert.False(schema.Tables[0].Columns[2].Nullable)
	assert.True(schema.Tables[0].Columns[2].Default)
	assert.Equal("updated_at", schema.Tables[0].Columns[3].Name)
	assert.Equal(reflect.TypeOf(time.Time{}), schema.Tables[0].Columns[3].RawType)
	assert.False(schema.Tables[0].Columns[3].Nullable)
	assert.True(schema.Tables[0].Columns[3].Default)
	assert.Equal("created_by", schema.Tables[0].Columns[4].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[0].Columns[4].RawType)
	assert.False(schema.Tables[0].Columns[4].Nullable)
	assert.True(schema.Tables[0].Columns[4].Default)
	assert.Equal("bottles", schema.Tables[1].Name)
	assert.Len(schema.Tables[1].Columns, 14)
	assert.Equal("id", schema.Tables[1].Columns[0].Name)
	assert.Equal(reflect.TypeOf(int64(0)), schema.Tables[1].Columns[0].RawType)
	assert.False(schema.Tables[1].Columns[0].Nullable)
	assert.True(schema.Tables[1].Columns[0].Default)
	assert.Equal("account_id", schema.Tables[1].Columns[1].Name)
	assert.Equal(reflect.TypeOf(int64(0)), schema.Tables[1].Columns[1].RawType)
	assert.False(schema.Tables[1].Columns[1].Nullable)
	assert.False(schema.Tables[1].Columns[1].Default)
	assert.Equal("rating", schema.Tables[1].Columns[2].Name)
	assert.Equal(reflect.TypeOf(int16(0)), schema.Tables[1].Columns[2].RawType)
	assert.False(schema.Tables[1].Columns[2].Nullable)
	assert.True(schema.Tables[1].Columns[2].Default)
	assert.Equal("name", schema.Tables[1].Columns[3].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[3].RawType)
	assert.False(schema.Tables[1].Columns[3].Nullable)
	assert.True(schema.Tables[1].Columns[3].Default)
	assert.Equal("vineyard", schema.Tables[1].Columns[4].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[4].RawType)
	assert.False(schema.Tables[1].Columns[4].Nullable)
	assert.True(schema.Tables[1].Columns[4].Default)
	assert.Equal("varietal", schema.Tables[1].Columns[5].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[5].RawType)
	assert.False(schema.Tables[1].Columns[5].Nullable)
	assert.True(schema.Tables[1].Columns[5].Default)
	assert.Equal("vintage", schema.Tables[1].Columns[6].Name)
	assert.Equal(reflect.TypeOf(int32(0)), schema.Tables[1].Columns[6].RawType)
	assert.False(schema.Tables[1].Columns[6].Nullable)
	assert.True(schema.Tables[1].Columns[6].Default)
	assert.Equal("color", schema.Tables[1].Columns[7].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[7].RawType)
	assert.False(schema.Tables[1].Columns[7].Nullable)
	assert.True(schema.Tables[1].Columns[7].Default)
	assert.Equal("sweetness", schema.Tables[1].Columns[8].Name)
	assert.Equal(reflect.TypeOf(int16(0)), schema.Tables[1].Columns[8].RawType)
	assert.True(schema.Tables[1].Columns[8].Nullable)
	assert.False(schema.Tables[1].Columns[8].Default)
	assert.Equal("country", schema.Tables[1].Columns[9].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[9].RawType)
	assert.True(schema.Tables[1].Columns[9].Nullable)
	assert.False(schema.Tables[1].Columns[9].Default)
	assert.Equal("region", schema.Tables[1].Columns[10].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[10].RawType)
	assert.True(schema.Tables[1].Columns[10].Nullable)
	assert.False(schema.Tables[1].Columns[10].Default)
	assert.Equal("review", schema.Tables[1].Columns[11].Name)
	assert.Equal(reflect.TypeOf(""), schema.Tables[1].Columns[11].RawType)
	assert.True(schema.Tables[1].Columns[11].Nullable)
	assert.False(schema.Tables[1].Columns[11].Default)
	assert.Equal("created_at", schema.Tables[1].Columns[12].Name)
	assert.Equal(reflect.TypeOf(time.Time{}), schema.Tables[1].Columns[12].RawType)
	assert.False(schema.Tables[1].Columns[12].Nullable)
	assert.True(schema.Tables[1].Columns[12].Default)
	assert.Equal("updated_at", schema.Tables[1].Columns[13].Name)
	assert.Equal(reflect.TypeOf(time.Time{}), schema.Tables[1].Columns[13].RawType)
	assert.False(schema.Tables[1].Columns[13].Nullable)
	assert.True(schema.Tables[1].Columns[13].Default)
}

func TestColumns_Column(t *testing.T) {
	t.Run("bigint", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "bigint",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(int64(0)),
		}, c.Column())
	})
	t.Run("bigint default", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: columnDefault("100"),
			IsNullable:    "NO",
			DataType:      "bigint",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(int64(0)),
			Default: true,
		}, c.Column())
	})
	t.Run("bigint nullable", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "YES",
			DataType:      "bigint",
		}
		assert.Equal(models.Column{
			Name:     "foo",
			RawType:  reflect.TypeOf(int64(0)),
			Nullable: true,
		}, c.Column())
	})
	t.Run("integer", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "integer",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(int32(0)),
		}, c.Column())
	})
	t.Run("smallint", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "smallint",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(int16(0)),
		}, c.Column())
	})
	t.Run("character", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "character",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(""),
		}, c.Column())
	})
	t.Run("character varying", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "character varying",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(""),
		}, c.Column())
	})
	t.Run("text", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "text",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(""),
		}, c.Column())
	})
	t.Run("boolean", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "boolean",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(false),
		}, c.Column())
	})
	t.Run("date", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "date",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(time.Time{}),
		}, c.Column())
	})
	t.Run("time without time zone", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "time without time zone",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(time.Time{}),
		}, c.Column())
	})
	t.Run("time with time zone", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "time with time zone",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(time.Time{}),
		}, c.Column())
	})
	t.Run("timestamp without time zone", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "timestamp without time zone",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(time.Time{}),
		}, c.Column())
	})
	t.Run("timestamp with time zone", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "timestamp with time zone",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(time.Time{}),
		}, c.Column())
	})
	t.Run("bytea", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "bytea",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf([]byte{}),
		}, c.Column())
	})
	t.Run("unknown", func(t *testing.T) {
		assert := assert.New(t)
		c := Columns{
			ColumnName:    "foo",
			ColumnDefault: nil,
			IsNullable:    "NO",
			DataType:      "type that we don't know",
		}
		assert.Equal(models.Column{
			Name:    "foo",
			RawType: reflect.TypeOf(new(interface{})).Elem(),
		}, c.Column())
	})
}

func columnDefault(s string) *string {
	return &s
}
