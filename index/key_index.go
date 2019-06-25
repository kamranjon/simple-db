package index

import (
	"fmt"
	"log"
	"strconv"

	"github.com/boltdb/bolt"
)

type KeyIndex struct {
	Db *bolt.DB
}

const indexPath = "key_index.db"
const bucketName = "KeyIndex"

func Open() (KeyIndex, error) {
	db, err := bolt.Open(indexPath, 0600, nil)
	if err != nil {
		return KeyIndex{}, err
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("Failed to create bucket: %s", err)
		}
		return nil
	})

	return KeyIndex{Db: db}, nil
}

func (k KeyIndex) Put(key string, value int64) error {
	err := k.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		s := strconv.FormatInt(value, 10)
		err := b.Put([]byte(key), []byte(s))
		return err
	})

	if err != nil {
		log.Println("Failed to add item to index:", err)
	}
	return nil
}

func (k KeyIndex) Get(key string) (int64, error) {
	var returnIndex int64
	err := k.Db.View(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte(bucketName))
		v := b.Get([]byte(key))
		if v == nil || len(v) == 0 { returnIndex = -1; return nil}
		returnIndex, err = strconv.ParseInt(string(v), 10, 64)
		if err != nil { return err }
		return nil
	})
	if err != nil { return int64(0), err }

	return returnIndex, nil

}

func (k KeyIndex) Delete(key string) error {
	if err := k.Db.Update(func(tx *bolt.Tx) error {
	    return tx.Bucket([]byte(bucketName)).Delete([]byte(key))
	}); err != nil {
	    return err
	}
	return nil
}
