package sorted_array

import "golang.org/x/exp/maps"

// ChunkStorage does simple CRUD operations on persistent storage
// Serialization(+compression) must be implemented at this level
type ChunkStorage interface {
	// Read if err is nil then it always return a map of size = len(chunkIds)
	Read(chunkIds []uint32) (map[uint32]*Chunk, error)
	Save(chunks map[uint32]*Chunk) error
	Remove(chunkIds []uint32) error

	ReadMeta() ([]*ChunkMeta, error)
	SaveMeta([]*ChunkMeta) error
}

type InMemoryChunkStorage struct {
	chunks map[uint32]*Chunk
	meta   []*ChunkMeta
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

func (s *InMemoryChunkStorage) ReadMeta() ([]*ChunkMeta, error) {
	return s.meta, nil
}

func (s *InMemoryChunkStorage) SaveMeta(meta []*ChunkMeta) error {
	s.meta = meta
	return nil
}

func NewInMemoryChunkStorage() *InMemoryChunkStorage {
	return &InMemoryChunkStorage{
		chunks: make(map[uint32]*Chunk),
	}
}
