package index

// Index defines an interface which stores integers (parquet offsets) based on strings
type Index interface {
	Put(string, int64) error
	Get(string) (int64, error)
	Delete(string) error
}