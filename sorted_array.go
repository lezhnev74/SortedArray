package sorted_array

import (
	"errors"
	"fmt"
)

var (
	noChunkFound = fmt.Errorf("no relevant chunk found")
)

// SortedArray manages ASC sorted array in chunks for better performance
// Chunks contain up to maxInsertSize items and may not intersect with each other
type SortedArray struct {
	name []byte

	lastChunkId                  int
	maxExtendSize, maxInsertSize int // chunk size allowed for extending boundaries and for insertion in the middle

	loadedChunks map[int]*Chunk
	dirtyChunks  map[int]struct{} // which loadedChunks are pending flushing

	meta      Meta // sorted array
	dirtyMeta bool // meta is pending flushing
}

// Add puts new values to the slice (protects from duplication)
func (i *SortedArray) Add(items []uint32) error {
	// Make a chunk map for new items (where to put each item)
	m := make(map[int][]uint32)
	for _, item := range items {
		relevantChunkId, err := i.selectRelevantChunkIdForInsertion(item)
		if err != nil {
			if errors.Is(err, noChunkFound) {
				relevantChunkId = i.createChunkFor(item)
			} else {
				return err
			}
		}

		s, ok := m[relevantChunkId]
		if !ok {
			m[relevantChunkId] = make([]uint32, 0, 1)
		}
		s = append(s, item)
	}

	// Process the map

	return nil
}

// ToSlice dump all index to a single slice (for debugging/testing)
func (i *SortedArray) ToSlice() []uint32 {
	return nil
}

func (i *SortedArray) selectRelevantChunkIdForInsertion(item uint32) (int, error) {
	return 0, nil
}

// createChunkFor allocates a new chunk for the item and puts it into
func (i *SortedArray) createChunkFor(item uint32) int {
	// Make a chunk
	chunkId := i.lastChunkId
	c := NewChunk([]uint32{item})

	// Update meta
	i.loadedChunks[chunkId] = c
	i.dirtyChunks[chunkId] = struct{}{}

	i.lastChunkId++
	i.dirtyMeta = true

	return chunkId
}

func (i *SortedArray) flush() {
	for id, _ := range i.dirtyChunks {
		// Flush the chunk to the storage
		delete(i.dirtyChunks, id)
	}
}

func NewIndex(name []byte) *SortedArray {
	return &SortedArray{
		name:          name,
		loadedChunks:  make(map[int]*Chunk),
		dirtyChunks:   make(map[int]struct{}),
		maxExtendSize: 1000,
		maxInsertSize: 2000,
	}
}
