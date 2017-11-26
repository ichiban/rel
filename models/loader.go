package models

type Loader interface {
	Load(*Schema) error
}
