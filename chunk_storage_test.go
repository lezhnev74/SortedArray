package sorted_array

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"sync"
	"testing"
	"time"
)

func TestInMemoryChunkStorage(t *testing.T) {
	storage := NewInMemoryChunkStorage()

	// Read:
	chunks, _ := storage.Read([]uint32{1, 2})
	require.Equal(t, 2, len(chunks))
	require.Nil(t, chunks[1])
	require.Nil(t, chunks[2])

	// Write:
	chunks[1] = NewChunk([]uint32{100, 200})
	chunks[2] = NewChunk([]uint32{300, 400})
	storage.Save(chunks)

	chunks2, _ := storage.Read([]uint32{1, 2})
	require.Equal(t, 2, len(chunks2))
	require.EqualValues(t, chunks, chunks2)

	// Remove:
	storage.Remove([]uint32{1})
	chunks3, _ := storage.Read([]uint32{1, 2})
	require.Equal(t, 2, len(chunks3))
	require.Nil(t, chunks3[1])
	require.EqualValues(t, chunks[2], chunks3[2])

	// Meta:
	list := &Meta{
		chunks: []*ChunkMeta{
			{1, 0, 2, 2},
			{2, 3, 4, 2},
		},
		nextId: 3,
	}
	storage.SaveMeta(list)
	list2, _ := storage.ReadMeta()
	require.EqualValues(t, list, list2)
}

func TestSqliteSortedArray(t *testing.T) {
	db := MakeSqliteDb()
	defer db.Close()

	// 1. TX1: Add/Remove/Flush
	tx, err := db.Begin()
	require.NoError(t, err)
	storage := NewSqliteTxSortedArrayStorage(tx, []byte("key1"))
	arr1 := NewSortedArray(2, storage)
	err = arr1.Add([]uint32{10, 20, 30, 40, 50})
	require.NoError(t, err)
	err = arr1.Delete([]uint32{10, 30, 50})
	require.NoError(t, err)
	require.EqualValues(t, []uint32{20, 40}, arr1.ToSlice())
	arr1.Flush()
	err = tx.Commit()
	require.NoError(t, err)

	// 2. Read
	tx, err = db.Begin()
	require.NoError(t, err)
	storage = NewSqliteTxSortedArrayStorage(tx, []byte("key1"))
	arr2 := NewSortedArray(2, storage)
	require.EqualValues(t, []uint32{20, 40}, arr2.ToSlice())
	err = tx.Commit()
	require.NoError(t, err)
}

func MakeSqliteDb() *sql.DB {
	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&_journal=WAL&_txlock=immediate")
	if err != nil {
		panic(err)
	}
	// Imitate table migration
	_, err = db.Exec(`CREATE TABLE sorted_array_chunks
(
    key   text PRIMARY KEY,
    chunk BLOB
)`)
	if err != nil {
		panic(err)
	}
	return db
}

func TestConcurrentWrites(t *testing.T) {
	// Attempt to update index concurrently
	db := MakeSqliteDb()
	defer db.Close()

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			var tx *sql.Tx
			var sqliteError sqlite3.Error
			for {
				tx, err = db.Begin()
				if errors.As(err, &sqliteError) && sqliteError.Code == 6 {
					time.Sleep(time.Millisecond * time.Duration(rand.Int()%10))
					continue
				}
				require.NoError(t, err)
				break
			}
			storage := NewSqliteTxSortedArrayStorage(tx, []byte("key1"))
			arr := NewSortedArray(2, storage)
			items := make([]uint32, rand.Uint32()%10_000)
			for i := 0; i < len(items); i++ {
				items[i] = rand.Uint32() % 1000
			}
			err = arr.Add(items)
			if err != nil {
				fmt.Printf("%v", err)
			}
			require.NoError(t, err)
			arr.Flush()
			err = tx.Commit()
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()
}
