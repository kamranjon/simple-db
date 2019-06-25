package db

import (
	"log"
	"os"
	"sort"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/reader"

	"github.com/kamranjon/simple-db/models"
	"github.com/kamranjon/simple-db/query"
	"github.com/kamranjon/simple-db/utils"
	"github.com/kamranjon/simple-db/index"
)

// ParquetDb is the parquet implementation of the store
type ParquetDb struct {
	Db
	Index index.KeyIndex
	StoreFile string
	SwapFile string
	StoreWriter *writer.ParquetWriter
	LocalFile source.ParquetFile
}

const StoreFile = "data_store.parquet"
const SwapFile = "swap_store.parquet"

// OpenParquetDb opens or creats a parquet file
func OpenParquetDb() (*ParquetDb, error) {
	keyIndex, err := index.Open()
	if err != nil {
		log.Println("Failed to create index:", err)
		return &ParquetDb{}, err
	}

	lf, pw, err := createParquetDbIfNotExist()
	if err != nil {
		log.Println("Failed to create DB:", err)
		return &ParquetDb{}, err
	}

	return &ParquetDb{
		Index: keyIndex,
		StoreFile: StoreFile,
		SwapFile: SwapFile,
		StoreWriter: pw,
		LocalFile: lf,
	}, nil
}

// Get takes a hash and returns record
func (db *ParquetDb) Get(viewHash string) (models.ViewEntry, error) {
	existingIndexPos, err := db.Index.Get(viewHash)
	if err != nil {
		return models.ViewEntry{}, err
	}

	fr, pr, err := db.getParquetReader()
	if err != nil { return models.ViewEntry{}, err }

	stus := make([]models.ViewEntry, 1)

	if (existingIndexPos != 0){
		pr.SkipRows(existingIndexPos)
	}

	if err = pr.Read(&stus); err != nil {
		log.Println("Failed to read from file:", err)
	}

	pr.ReadStop()

	fr.Close()

	return stus[0], nil
}

// Put takes a models.ViewEntry and stores it in the db - rewrites db if upsert occurs
func (db *ParquetDb) Put(viewEntry models.ViewEntry) error {
	viewHash := viewEntry.GenerateKey()

	existingIndexPos, err := db.Index.Get(viewHash)
	if err != nil {
		return err
	}
	// if it doesn't exist in the index - we write without closing our connection
	if existingIndexPos == -1 {
		file, _ := os.Open(StoreFile)
		stats, _ := file.Stat()
		size := stats.Size()

		// only attempt to read if file has data
		totalRows := int64(0)
		if size > 5 {
			fr, pr, err := db.getParquetReader()
			if err != nil {
				log.Println("Failed to create parquet reader:", err)
				return err
			}

			totalRows = int64(pr.GetNumRows())
			pr.ReadStop()
			fr.Close()
		}

		if err := db.StoreWriter.Write(viewEntry); err != nil {
			log.Println("Failed to write to Parquet file:", err)
			return err
		}
		log.Println("Putting index:", viewHash)
		err = db.Index.Put(viewHash, totalRows)
		if err != nil {
			return err
		}
	// otherwise replicate on every upsert
	} else {

		// Replicate DB
		totalRows, err := db.Replicate(existingIndexPos)
		if err != nil {
			log.Println("Failed to replicate datastore:", err)
			return err
		}

		// Write new entry
		if err := db.StoreWriter.Write(viewEntry); err != nil {
			log.Println("Failed to write new entry to store:", err)
			return err
		}

		// Update index
		err = db.Index.Put(viewEntry.GenerateKey(), totalRows - 1)
		if err != nil {
			return err
		}

		// write changes
		err = db.WriteOut()
		if err != nil { return err }
		// swap db files
		err = db.swapFiles()
		if err != nil { return err }
		// create new db
		db.LocalFile, db.StoreWriter, err = createParquetDbIfNotExist()
		if err != nil {
			return err
		}

	}

	return nil
}

// Delete takes hash and deletes record
func (db *ParquetDb) Delete(viewHash string) error {
	//TODO: rebuild partial index after delete
	var err error
	// attach to db
	db.LocalFile, db.StoreWriter, err = createParquetDbIfNotExist()
	if err != nil {
		return err
	}

	existingIndexPos, err := db.Index.Get(viewHash)
	if err != nil {
		return err
	}

	// Replicate DB without given item
	_, err = db.Replicate(existingIndexPos)
	if err != nil {
		log.Println("Error replicating datastore:", err)
		return err
	}

	// write changes
	err = db.WriteOut()
	if err != nil { return err }

	// remove entry from index
	err = db.Index.Delete(viewHash)
	if err != nil { return err }

	// swap db files
	err = db.swapFiles()
	if err != nil { return err }

	return nil
}


// Query takes a Query struct and returns records
func (db *ParquetDb) Query(query query.Query) []models.ViewEntry {
	// read all records in (not optimized for memory)
	fr, pr, err := db.getParquetReader()
	if err != nil {
		return []models.ViewEntry{}
	}

	num := int64(pr.GetNumRows())
	results := make([]models.ViewEntry, num)

	if err = pr.Read(&results); err != nil {
		log.Println("Failed to read from local file:", err)
		return []models.ViewEntry{}
	}

	for key, value := range query.FilterClause {
		results = utils.Filter(results, func(ve models.ViewEntry) bool { return ve.OutputMap()[key] == value })
	}

	for _, value := range query.OrderColumns {
		switch value {
		case "stb":
			sort.Sort(models.ByStb(results))
		case "title":
			sort.Sort(models.ByTitle(results))
		case "provider":
			sort.Sort(models.ByProvider(results))
		case "date":
			sort.Sort(models.ByDate(results))
		case "rev":
			sort.Sort(models.ByRev(results))
		case "view_time":
			sort.Sort(models.ByViewTime(results))
		}
	}

	pr.ReadStop()
	fr.Close()
	return results
}

// WriteOut writes changes to disk
func (db *ParquetDb) WriteOut() error {
	var err error
	if err = db.StoreWriter.WriteStop(); err != nil {
		log.Println("Failed to stop writer:", err)
		return err
	}
	db.LocalFile.Close()

	return nil
}

// Replicate copies store to new file - does not close writer
func (db *ParquetDb) Replicate(skipIndexes ...int64) (int64, error) {
	fr, pr, err := db.getParquetReader()
	if err != nil {
		return 0, err
	}

	// iterate over current parquet store - transfer it to new file
	num := int64(pr.GetNumRows())
	skipped := int64(0)

	for i := int64(0); i < num; i++ {

		ve := make([]models.ViewEntry, 1)

		if err = pr.Read(&ve); err != nil {
			log.Println("Failed to read from local file:", err)
			return num, err
		}

		// skip any indexes that we are upserting
		if utils.Contains(skipIndexes, int64(i)) {
			skipped++
			continue
		}

		newIndex := i - skipped

		if err := db.StoreWriter.Write(ve[0]); err != nil {
			log.Println("Failed to write to local file:", err)
			return num, err
		}

		if err := db.Index.Put(ve[0].GenerateKey(), newIndex); err != nil {
			log.Println("Failed to update index:", err)
			return num, err
		}
	}

	pr.ReadStop()
	fr.Close()

	return num, nil
}

func createParquetDbIfNotExist() (source.ParquetFile, *writer.ParquetWriter, error) {
	var pw *writer.ParquetWriter
	var lf source.ParquetFile
	// if the store doesn't exist - write to main store file
	if _, err := os.Stat(StoreFile); os.IsNotExist(err) {
		lf, pw, err = createParquetDb(StoreFile)
		if err != nil { return lf, pw, err }
	// otherwise write to the swap file
	} else {
		lf, pw, err = createParquetDb(SwapFile)
		if err != nil { return lf, pw, err }
	}
	return lf, pw, nil
}

func createParquetDb(file string) (source.ParquetFile, *writer.ParquetWriter, error) {
	lf, err := local.NewLocalFileWriter(file)
	if err != nil {
		log.Println("Failed to create local file:", err)
		return lf, &writer.ParquetWriter{}, err
	}

	pw, err := writer.NewParquetWriter(lf, new(models.ViewEntry), 4)
	if err != nil {
		log.Println("Failed to create parquet writer:", err)
		return lf, pw, err
	}
	return lf, pw, nil
}

func (db *ParquetDb) getParquetReader() (source.ParquetFile, *reader.ParquetReader, error) {
	fr, err := local.NewLocalFileReader(db.StoreFile)
	if err != nil {
		log.Println("Failed to open local file:", err)
		return fr, &reader.ParquetReader{}, err
	}

	pr, err := reader.NewParquetReader(fr, new(models.ViewEntry), 4)
	if err != nil {
		log.Println("Failed to create parquet reader:", err)
		return fr, pr, err
	}

	return fr, pr, nil
}

func (db *ParquetDb) swapFiles() error {
	// Remove old store
	if err := os.Remove(StoreFile); err != nil {
		log.Println("Failed to delete store file:", err)
		return err
	}
	// Rename new store
	if err := os.Rename(SwapFile, StoreFile); err != nil {
		log.Println("Failed to rename swap file:", err)
		return err
	}
	return nil
}