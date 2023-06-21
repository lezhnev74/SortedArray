package sorted_array

import (
	"errors"
	"fmt"
	"golang.org/x/exp/maps"
)

var (
	noChunkFound = fmt.Errorf("no relevant chunk found")
	chunkTooBig  = fmt.Errorf("relevant chunk is too big")
)

// SortedArray manages ASC sorted array in chunks for better performance
// Chunks contain up to maxInsertSize items and may not intersect with each other
type SortedArray struct {
	name         []byte
	maxChunkSize uint32
	loadedChunks map[uint32]*Chunk
	dirtyChunks  map[uint32]struct{} // which loadedChunks are pending flushing
	meta         Meta                // sorted array
	dirtyMeta    bool                // meta is pending flushing
	storage      ChunkStorage
}

func (a *SortedArray) GetInRange(min, max uint32) ([]uint32, error) {
	// 1. See appropriate chunks in meta
	relevantChunkMeta := a.meta.FindRelevantForReadRange(min, max)
	chunkIds := make([]uint32, len(relevantChunkMeta))
	for i, cm := range relevantChunkMeta {
		chunkIds[i] = cm.id
	}
	a.loadMissingChunks(chunkIds)

	// 2. Copy all appropriate ids from loaded chunks
	ret := make([]uint32, 0)
	for _, id := range chunkIds {
		for _, item := range a.loadedChunks[id].Items {
			if item >= min && item <= max {
				ret = append(ret, item)
			}
		}
	}

	return ret, nil
}

// Add puts new values to the slice (protects from duplication)
func (a *SortedArray) Add(items []uint32) error {
	// Make a chunk map for new items (where to put each item) - a modification plan
	plan := make(map[uint32][]uint32)
	for _, item := range items {
		relevantChunkId, err := a.selectChunkIdForInsertion(item)
		if err != nil {
			if errors.Is(err, noChunkFound) {
				relevantChunkId = a.createChunkFor(item)
			} else {
				return err
			}
		}
		_, ok := plan[relevantChunkId]
		if !ok {
			plan[relevantChunkId] = make([]uint32, 0, 1)
		}
		plan[relevantChunkId] = append(plan[relevantChunkId], item)
	}

	// Process the map
	// 1. Load missing chunks
	a.loadMissingChunks(maps.Keys(plan))
	// 2. Make insertion
	for chunkId, items := range plan {
		a.loadedChunks[chunkId].Add(items)
		// update meta
		cm := a.meta.GetChunkById(chunkId)
		cm.min = a.loadedChunks[chunkId].Items[0]
		cm.max = a.loadedChunks[chunkId].Items[len(a.loadedChunks[chunkId].Items)-1]
		cm.size = uint32(len(a.loadedChunks[chunkId].Items))
		a.dirtyMeta = true
	}

	return nil
}

// ToSlice dump all index to a single slice (for debugging/testing)
func (a *SortedArray) ToSlice() []uint32 {

	ret := make([]uint32, 0, len(a.meta.chunks))
	for _, cm := range a.meta.chunks {
		chunk := a.loadedChunks[cm.id]
		ret = append(ret, chunk.Items...)
	}

	return ret
}

// selectChunkIdForInsertion finds a suitable chunk for storing this item in meta
func (a *SortedArray) selectChunkIdForInsertion(item uint32) (chunkId uint32, err error) {
	cms := a.meta.FindRelevantForInsert(item)
	// 0. No suitable chunks -> create
	if len(cms) == 0 {
		err = noChunkFound
		return
	}
	// 1. One chunk -> use
	if len(cms) == 1 {
		chunkId = cms[0].id
		if cms[0].size >= a.maxChunkSize {
			err = chunkTooBig
		}
		return
	}
	// 2. Two chunks -> select most appropriate
	if cms[0].size < cms[1].size {
		chunkId = cms[0].id
		if cms[0].size >= a.maxChunkSize {
			err = chunkTooBig
		}
		return
	}

	chunkId = cms[1].id
	if cms[1].size >= a.maxChunkSize {
		err = chunkTooBig
	}
	return
}

// createChunkFor allocates a new chunk for the item and puts it into
func (a *SortedArray) createChunkFor(item uint32) uint32 {
	// Make Chunk Description
	chunkId := a.meta.TakeNextId()
	chunkMeta := &ChunkMeta{chunkId, item, item, 1}
	a.meta.Add([]*ChunkMeta{chunkMeta})
	a.dirtyMeta = true

	// Make a chunk
	c := NewChunk([]uint32{item})
	a.loadedChunks[chunkId] = c
	a.dirtyChunks[chunkId] = struct{}{}

	return chunkId
}

// loadMissingChunks checks which chunks are not in memory and loads them from the storage
func (a *SortedArray) loadMissingChunks(ids []uint32) error {
	// 1. remove already loaded
	for i, id := range ids {
		_, exists := a.loadedChunks[id]
		if !exists {
			continue
		}
		if i < len(ids)-1 {
			ids = append(ids[:i], ids[i+1:]...)
		} else {
			ids = ids[:i]
		}
	}
	// 2. Load the rest
	loaded, err := a.storage.Read(ids)
	if err != nil {
		return err
	}
	// 3. Merge with the existing load
	maps.Copy(a.loadedChunks, loaded)
	return nil
}

func (a *SortedArray) flush() {
	// dirty meta
	for id, _ := range a.dirtyChunks {
		// Flush the chunk to the storage
		delete(a.dirtyChunks, id)
	}
}

func NewSortedArray(name []byte, maxChunkSize uint32, s ChunkStorage) *SortedArray {
	return &SortedArray{
		name:         name,
		loadedChunks: make(map[uint32]*Chunk),
		dirtyChunks:  make(map[uint32]struct{}),
		maxChunkSize: maxChunkSize,
		storage:      s,
	}
}
