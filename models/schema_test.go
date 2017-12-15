package models

import (
	"reflect"
	"testing"
)

func TestSchema_Columns(t *testing.T) {
	testCases := []struct {
		schema  Schema
		columns []Column
	}{
		{
			schema: Schema{
				Package: "models",
				Tables: []Table{
					{
						Name: "Foo",
					},
				},
			},
			columns: nil,
		},
		{
			schema: Schema{
				Package: "models",
				Tables: []Table{
					{
						Name: "Foo",
						Columns: []Column{
							{Name: "id", RawType: reflect.TypeOf(int64(0)), Default: true},
							{Name: "name", RawType: reflect.TypeOf("")},
						},
					},
				},
			},
			columns: []Column{
				{Name: "id"},
				{Name: "name"},
			},
		},
		{
			schema: Schema{
				Package: "models",
				Tables: []Table{
					{
						Name: "Foo",
						Columns: []Column{
							{Name: "id", RawType: reflect.TypeOf(int64(0)), Default: true},
							{Name: "name", RawType: reflect.TypeOf("")},
						},
					},
					{
						Name: "Bar",
						Columns: []Column{
							{Name: "id", RawType: reflect.TypeOf(int64(0)), Default: true},
							{Name: "email", RawType: reflect.TypeOf("")},
						},
					},
				},
			},
			columns: []Column{
				{Name: "id"},
				{Name: "name"},
				{Name: "email"},
			},
		},
	}

	for _, tc := range testCases {
		cs := tc.schema.Columns()

		if len(tc.columns) != len(cs) {
			t.Errorf("expected: %d, got: %d", len(tc.columns), len(cs))
		}

		for i, c := range tc.columns {
			if c.Name != cs[i].Name {
				t.Errorf("expected: %s, got: %s", c.Name, cs[i].Name)
			}
		}
	}
}

func TestSchema_WriteTo(t *testing.T) {
	// TODO:
}
