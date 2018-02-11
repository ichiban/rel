package rel

type Loader interface {
	Load(*Schema) error
}
