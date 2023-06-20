package sorted_array

import "golang.org/x/exp/maps"

// ChunkStorage does simple CRUD operations on persistent storage
// Serialization(+compression) must be implemented at this level
type ChunkStorage interface {
	// Read if err is nil then it always return a map of size = len(chunkIds)
	Read(chunkIds []int) (map[int]*Chunk, error)
	Save(chunks map[int]*Chunk) error
	Remove(chunkIds []int) error

	ReadMeta() ([]ChunkMeta, error)
	SaveMeta([]ChunkMeta) error
}

type InMemoryChunkStorage struct {
	chunks map[int]*Chunk
	meta   []ChunkMeta
}

func (s *InMemoryChunkStorage) Read(chunkIds []int) (map[int]*Chunk, error) {
	chunks := make(map[int]*Chunk, len(chunkIds))
	for _, id := range chunkIds {
		if _, ok := s.chunks[id]; ok {
			chunks[id] = s.chunks[id]
		} else {
			chunks[id] = nil
		}
	}
	return chunks, nil
}

func (s *InMemoryChunkStorage) Remove(chunkIds []int) error {
	for _, id := range chunkIds {
		delete(s.chunks, id)
	}
	return nil
}

func (s *InMemoryChunkStorage) Save(chunks map[int]*Chunk) error {
	maps.Copy(s.chunks, chunks)
	return nil
}

func (s *InMemoryChunkStorage) ReadMeta() ([]ChunkMeta, error) {
	return s.meta, nil
}

func (s *InMemoryChunkStorage) SaveMeta(meta []ChunkMeta) error {
	s.meta = meta
	return nil
}

func NewInMemoryChunkStorage() *InMemoryChunkStorage {
	return &InMemoryChunkStorage{
		chunks: make(map[int]*Chunk),
	}
}
