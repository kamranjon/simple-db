package db

import (
	"github.com/kamranjon/simple-db/models"
	"github.com/kamranjon/simple-db/query"
)

type Db interface {
	Put(models.ViewEntry) error
	Get(string) (models.ViewEntry, error)
	Delete(string) error
	Query(query.Query) []models.ViewEntry
}