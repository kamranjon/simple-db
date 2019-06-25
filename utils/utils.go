package utils

import(
	"github.com/kamranjon/simple-db/models"
)

func Contains(s []int64, e int64) bool {
  for _, a := range s {
    if a == e { return true }
  }
  return false
}

func Filter(vs []models.ViewEntry, f func(models.ViewEntry) bool) []models.ViewEntry {
  vsf := make([]models.ViewEntry, 0)
  for _, v := range vs {
    if f(v) {
      vsf = append(vsf, v)
    }
  }
  return vsf
}
