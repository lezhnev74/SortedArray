package sorted_array

import (
	"database/sql"
	"errors"
	"fmt"
	errors2 "github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

// ChunkStorage does simple CRUD operations on persistent storage
// Serialization(+compression) must be implemented at this level
type ChunkStorage interface {
	// Read if err is nil then it always return a map of size = len(chunkIds)
	Read(chunkIds []uint32) (map[uint32]*Chunk, error)
	Save(chunks map[uint32]*Chunk) error
	Remove(chunkIds []uint32) error

	ReadMeta() (*Meta, error)
	SaveMeta(*Meta) error
}

type InMemoryChunkStorage struct {
	chunks map[uint32]*Chunk
	meta   *Meta
}

func (s *InMemoryChunkStorage) Read(chunkIds []uint32) (map[uint32]*Chunk, error) {
	chunks := make(map[uint32]*Chunk, len(chunkIds))
	for _, id := range chunkIds {
		if _, ok := s.chunks[id]; ok {
			chunks[id] = s.chunks[id]
		} else {
			chunks[id] = nil
		}
	}
	return chunks, nil
}

func (s *InMemoryChunkStorage) Remove(chunkIds []uint32) error {
	for _, id := range chunkIds {
		delete(s.chunks, id)
	}
	return nil
}

func (s *InMemoryChunkStorage) Save(chunks map[uint32]*Chunk) error {
	maps.Copy(s.chunks, chunks)
	return nil
}

func (s *InMemoryChunkStorage) ReadMeta() (*Meta, error) {
	m := s.meta
	if m == nil {
		return &Meta{0, nil}, nil
	}
	return m, nil
}

func (s *InMemoryChunkStorage) SaveMeta(meta *Meta) error {
	s.meta = meta
	return nil
}

func NewInMemoryChunkStorage() *InMemoryChunkStorage {
	return &InMemoryChunkStorage{
		chunks: make(map[uint32]*Chunk),
	}
}

// SortedArraySqlTxStorage implemented sorted array storage for sqlite
// it uses blobs to store chunks and meta
// key is used to produce unique ids for the blobs in a shared table
type SortedArraySqlTxStorage struct {
	key []byte // id of the array in the storage
	// SQLite is NOT threadsafe for writes, so any write can actually return "table is locked"
	// so to mitigate this it is better to start transaction IMMEDIATELY (instead of lazy transactions)
	// handle "table is locked" at db.Begin() call so the rest is 100% thread-safe
	tx             *sql.Tx // a tx to work within
	preparedRemove *sql.Stmt
	preparedUpsert *sql.Stmt
	preparedRead   *sql.Stmt
}

func (s *SortedArraySqlTxStorage) Read(chunkIds []uint32) (map[uint32]*Chunk, error) {
	ret := make(map[uint32]*Chunk, 0)
	for _, id := range chunkIds {
		r := s.preparedRead.QueryRow(s.chunkId(id))
		var serialized []byte
		err := r.Scan(&serialized)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		} else if err != nil {
			return nil, errors2.Wrap(err, "Read:")
		}
		ret[id], err = UnserializeChunk(serialized)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
func (s *SortedArraySqlTxStorage) Save(chunks map[uint32]*Chunk) error {
	for id, chunk := range chunks {
		chunkSerialized, err := chunk.Serialize()
		if err != nil {
			return err
		}
		s.preparedUpsert.Exec(s.chunkId(id), chunkSerialized)
	}
	return nil
}
func (s *SortedArraySqlTxStorage) Remove(chunkIds []uint32) error {
	for _, id := range chunkIds {
		_, err := s.preparedRemove.Exec(id)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *SortedArraySqlTxStorage) ReadMeta() (*Meta, error) {
	r := s.preparedRead.QueryRow(s.key)
	var serialized []byte
	err := r.Scan(&serialized)

	if errors.Is(err, sql.ErrNoRows) {
		return NewMeta(), nil
	} else if err != nil {
		return nil, err
	}
	return UnserializeMeta(serialized)
}
func (s *SortedArraySqlTxStorage) SaveMeta(meta *Meta) error {
	serialized, err := meta.Serialize()
	if err != nil {
		return err
	}
	s.preparedUpsert.Exec(s.key, serialized)
	return nil
}
func (a *SortedArraySqlTxStorage) chunkId(id uint32) []byte {
	return []byte(fmt.Sprintf("%s_%d", a.key, id))
}

func NewSqliteTxSortedArrayStorage(tx *sql.Tx, key []byte) *SortedArraySqlTxStorage {
	prepWrite, err := tx.Prepare("INSERT OR REPLACE INTO sorted_array_chunks(key,chunk) VALUES(?,?)")
	if err != nil {
		panic(err)
	}
	prepRead, err := tx.Prepare("SELECT chunk FROM sorted_array_chunks WHERE key=?")
	if err != nil {
		panic(err)
	}
	prepRemove, err := tx.Prepare("DELETE FROM sorted_array_chunks WHERE key=?")
	if err != nil {
		panic(err)
	}
	return &SortedArraySqlTxStorage{
		key:            key,
		tx:             tx,
		preparedRemove: prepRemove,
		preparedUpsert: prepWrite,
		preparedRead:   prepRead,
	}
}
