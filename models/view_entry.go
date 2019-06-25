package models

import (
	"fmt"
	"strconv"
	"hash/fnv"
)

// ViewEntry is the model for entries in the database
type ViewEntry struct {
	Stb string `parquet:"name=stb, type=UTF8"`
	Title string `parquet:"name=title, type=UTF8"`
	Provider string `parquet:"name=provider, type=UTF8"`
	Date int64 `parquet:"name=date, type=TIMESTAMP_MILLIS"`
	Rev float64 `parquet:"name=rev, type=DOUBLE"`
	ViewTime int32 `parquet:"name=view_time, type=TIME_MILLIS"`
}

// GenerateKey returns a hash of unique values for indexing
func (v ViewEntry) GenerateKey() string {
	dateString := strconv.FormatInt(v.Date, 10)
	uniqueString := fmt.Sprintf("%s%s%s", v.Stb, v.Title, dateString)
	hash := fnv.New64a()
	hash.Write([]byte(uniqueString))
	return fmt.Sprintf("%x", hash.Sum64())
}

// OutputMap returns a map representation of a ViewEntry
func (v ViewEntry) OutputMap() map[string]interface{} {
	return map[string]interface{}{
		"stb": v.Stb,
		"title": v.Title,
		"provider": v.Provider,
		"date": v.Date,
		"rev": v.Rev,
		"view_time": v.ViewTime,
	}
}


// ByStb sorts by stb
type ByStb []ViewEntry

func (a ByStb) Len() int           { return len(a) }
func (a ByStb) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByStb) Less(i, j int) bool { return a[i].Stb < a[j].Stb }

// ByTitle sorts by title
type ByTitle []ViewEntry

func (a ByTitle) Len() int           { return len(a) }
func (a ByTitle) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTitle) Less(i, j int) bool { return a[i].Title < a[j].Title }

// ByProvider sorts by provider
type ByProvider []ViewEntry

func (a ByProvider) Len() int           { return len(a) }
func (a ByProvider) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByProvider) Less(i, j int) bool { return a[i].Provider < a[j].Provider }

// ByDate sorts by Date
type ByDate []ViewEntry

func (a ByDate) Len() int           { return len(a) }
func (a ByDate) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDate) Less(i, j int) bool { return a[i].Date < a[j].Date }

// ByRev sorts by rev
type ByRev []ViewEntry

func (a ByRev) Len() int           { return len(a) }
func (a ByRev) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByRev) Less(i, j int) bool { return a[i].Rev < a[j].Rev }

type ByViewTime []ViewEntry

func (a ByViewTime) Len() int           { return len(a) }
func (a ByViewTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByViewTime) Less(i, j int) bool { return a[i].ViewTime < a[j].ViewTime }