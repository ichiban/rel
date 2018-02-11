package rel

type Table struct {
	Name       string
	Columns    []Column
	PrimaryKey []Column
}
