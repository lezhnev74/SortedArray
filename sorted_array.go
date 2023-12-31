package sorted_array

import (
	"fmt"
	sorted_numeric_streams "github.com/lezhnev74/SetOperationsOnSortedNumericStreams"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"math"
	"sync"
)

var (
	noChunkFound = fmt.Errorf("no relevant chunk found")
	chunkTooBig  = fmt.Errorf("relevant chunk is too big")
)

// SortedArray manages ASC sorted array in chunks for better performance
// Chunks contain up to maxInsertSize items and may not intersect with each other
type SortedArray struct {
	maxChunkSize uint32
	chunksLock   sync.Mutex
	loadedChunks map[uint32]*Chunk
	dirtyChunks  map[uint32]struct{} // which loadedChunks are pending flushing
	meta         *Meta               // sorted array
	dirtyMeta    bool                // meta is pending flushing
	metaInit     bool                // meta is loaded from storage
	storage      ChunkStorage
}

// GetInRange returns a stream of items (min,max are INCLUDED)
func (a *SortedArray) GetInRange(min, max uint32) (sorted_numeric_streams.SortedNumbersStream[uint32], error) {
	err := a.initMeta()
	if err != nil {
		return nil, err
	}

	// 1. See appropriate chunks in meta
	relevantChunkMeta := a.meta.FindRelevantForReadRange(min, max)

	result := sorted_numeric_streams.NewChannelStream[uint32]()
	go func() {
		defer result.Close()
		// 2. Iterate over all chunks in order and push items to the outbound stream
		for _, cm := range relevantChunkMeta {
			err := a.loadChunks([]uint32{cm.id}) // load the chunk
			if err != nil {
				panic(err)
			}
			for _, item := range a.loadedChunks[cm.id].Items {
				if item >= min && item <= max {
					result.Push(item)
				}
			}
			a.releaseChunks([]uint32{cm.id})
		}
	}()
	return result, nil
}

func (a *SortedArray) Delete(items []uint32) error {
	err := a.initMeta()
	if err != nil {
		return err
	}
	// 1. Plan. Make a chunk map for new items (where to put each item) - a modification plan
	plan, err := a.planModification(items)
	if err != nil {
		return err
	}
	// 2. Load missing chunks
	err = a.loadChunks(maps.Keys(plan))
	if err != nil {
		return err
	}
	// 3. Make removal
	emptyChunkIds := make([]uint32, 0)
	for chunkId, items := range plan {
		chunk := a.loadedChunks[chunkId]
		removed := chunk.Remove(items)
		if removed == 0 {
			continue
		}
		// detect empty chunk
		if len(chunk.Items) == 0 {
			emptyChunkIds = append(emptyChunkIds, chunkId)
			continue
		}
		a.dirtyChunks[chunkId] = struct{}{}
		// update meta
		a.dirtyMeta = true
		cm := a.meta.GetChunkById(chunkId)
		cm.min = chunk.Items[0]
		cm.max = chunk.Items[len(chunk.Items)-1]
		cm.size = uint32(len(chunk.Items))
	}
	// 4. Cleanup empty
	a.storage.Remove(emptyChunkIds)
	for _, chunkId := range emptyChunkIds {
		a.meta.Remove(a.meta.GetChunkById(chunkId))
	}
	// 5. Detect too small chunks and MERGE those
	a.merge()
	return nil
}

// Add Puts new items to the array
func (a *SortedArray) Add(items []uint32) error {
	if len(items) == 0 {
		return nil
	}
	err := a.initMeta()
	if err != nil {
		return err
	}

	// 0. edge-case: the birth of the index, first chunk is created here
	// all further chunks are made by SPLITTING only
	if len(a.meta.chunks) == 0 {
		a.createChunkFor(items[:1])
		items = items[1:]    // the first item was consumed to spawn a new chunk
		if len(items) == 0 { // another check after consuming one item
			return nil
		}
	}
	// 1. Make a chunk map for new items (where to put each item) - a modification plan
	plan, err := a.planModification(items)
	if err != nil {
		return err
	}
	// 2. Load missing chunks
	err = a.loadChunks(maps.Keys(plan))
	if err != nil {
		return err
	}
	// 3. Make insertion
	for chunkId, items := range plan {
		added := a.loadedChunks[chunkId].Add(items)
		if added == 0 {
			continue // no new items added
		}
		a.dirtyChunks[chunkId] = struct{}{}
		// update meta
		cm := a.meta.GetChunkById(chunkId)
		cm.size += uint32(added)
		if a.loadedChunks[chunkId].Items[0] < cm.min {
			cm.min = a.loadedChunks[chunkId].Items[0]
		}
		if a.loadedChunks[chunkId].Items[len(a.loadedChunks[chunkId].Items)-1] > cm.max {
			cm.max = a.loadedChunks[chunkId].Items[len(a.loadedChunks[chunkId].Items)-1]
		}
		a.dirtyMeta = true
	}
	// 4 Detect Too Big chunks and split those
	a.split()
	return nil
}

// ToSlice dump all index to a single slice (for debugging/testing)
func (a *SortedArray) ToSlice() []uint32 {
	err := a.initMeta()
	if err != nil {
		panic(err)
	}
	size := uint32(0)
	ids := make([]uint32, 0, len(a.meta.chunks))
	for _, cm := range a.meta.chunks {
		size += cm.size
		ids = append(ids, cm.id)
	}
	err = a.loadChunks(ids)
	if err != nil {
		panic(errors.Wrap(err, "ToSlice() failed"))
	}
	ret := make([]uint32, 0, size)
	for _, cm := range a.meta.chunks {
		chunk := a.loadedChunks[cm.id]
		ret = append(ret, chunk.Items...)
	}

	return ret
}
func (a *SortedArray) dumpChunks() {
	fmt.Printf("--- chunks ---\n")
	for _, cm := range a.meta.chunks {
		fmt.Printf("chunk %d: %v\n", cm.id, a.loadedChunks[cm.id].Items)
	}
}
func (a *SortedArray) getChunks() (chunks [][]uint32) {
	for _, cm := range a.meta.chunks {
		chunks = append(chunks, a.loadedChunks[cm.id].Items)
	}
	return
}

// planModification returns items grouped by relevant chunk
func (a *SortedArray) planModification(items []uint32) (plan map[uint32][]uint32, err error) {
	plan = make(map[uint32][]uint32)
	for _, item := range items {
		relevantChunkId, err := a.selectChunkIdForInsertion(item)
		if err != nil {
			return nil, err
		}
		_, ok := plan[relevantChunkId]
		if !ok {
			plan[relevantChunkId] = make([]uint32, 0, 1)
		}
		plan[relevantChunkId] = append(plan[relevantChunkId], item)
	}
	return plan, nil
}

// selectChunkIdForInsertion finds a suitable chunk for storing this item
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
		return
	}
	// 2. Two chunks -> select most appropriate
	if cms[0].size < cms[1].size {
		chunkId = cms[0].id
	} else {
		chunkId = cms[1].id
	}
	return
}

// createChunkFor allocates a new chunk for the item and puts it into
// items are sorted
func (a *SortedArray) createChunkFor(items []uint32) uint32 {
	// Make Chunk Description
	chunkId := a.meta.TakeNextId()
	chunkMeta := &ChunkMeta{chunkId, items[0], items[len(items)-1], uint32(len(items))}
	a.meta.Add([]*ChunkMeta{chunkMeta})
	a.dirtyMeta = true

	// Make a chunk
	c := NewChunk(items)
	a.loadedChunks[chunkId] = c
	a.dirtyChunks[chunkId] = struct{}{}

	return chunkId
}

// loadChunks checks which chunks are not in memory and loads them from the storage
func (a *SortedArray) loadChunks(ids []uint32) error {
	a.chunksLock.Lock()
	defer a.chunksLock.Unlock()
	// 1. remove already loaded
	i := 0
	for _, id := range ids {
		if _, exists := a.loadedChunks[id]; !exists {
			ids[i] = id
			i++
		}
	}
	ids = ids[:i]
	if len(ids) == 0 {
		return nil
	}
	// 2. Load the rest
	loaded, err := a.storage.Read(ids)
	if err != nil {
		return err
	}
	// 3. merge with the existing load
	maps.Copy(a.loadedChunks, loaded)
	return nil
}

// releaseChunks removes pointers to chunk instances for later GC
func (a *SortedArray) releaseChunks(ids []uint32) {
	a.chunksLock.Lock()
	defer a.chunksLock.Unlock()
	for _, id := range ids {
		delete(a.loadedChunks, id)
	}
}

func (a *SortedArray) Flush() error {
	if a.dirtyMeta {
		a.dirtyMeta = false
		a.storage.SaveMeta(a.meta)
	}
	chunksToSave := make(map[uint32]*Chunk, 0)
	for id, _ := range a.dirtyChunks {
		chunksToSave[id] = a.loadedChunks[id]
		delete(a.dirtyChunks, id)
		delete(a.loadedChunks, id) // free the chunk
	}
	return a.storage.Save(chunksToSave)
}

// split detects Too Big chunks based on Meta and split those
// Redistribute affected items within split chunks
// Return true if at least one split happened
func (a *SortedArray) split() (split bool) {
	for _, cm := range a.meta.chunks {
		// Check SPLIT conditions
		if cm.size <= a.maxChunkSize { // means only SPLIT when overflow actually happens
			continue
		}
		// SPLIT:
		split = true
		chunk := a.loadedChunks[cm.id]
		newSize := uint32(math.Ceil(float64(cm.size) / 2))
		newChunkItems := chunk.Items[newSize:] // split in half
		chunk.Items = chunk.Items[:newSize]
		// Update original chunk's meta
		cm.size = newSize
		cm.max = chunk.Items[newSize-1]
		// Create a new chunk
		a.createChunkFor(newChunkItems)
	}
	if split {
		return a.split() // go on until no more to split
	}
	return
}

func (a *SortedArray) merge() {
	// 1. Make a merge plan:
	plan := make([][]*ChunkMeta, 0) // each item contains two pieces to merge (ordered)
	for i := 1; i < len(a.meta.chunks); i++ {
		cm := a.meta.chunks[i]
		prevCm := a.meta.chunks[i-1]
		mergeSize := cm.size + prevCm.size
		if mergeSize > a.maxChunkSize {
			continue
		}
		plan = append(plan, []*ChunkMeta{prevCm, cm}) // ordered
		i++                                           // skip the processed one
	}

	// 2. Load all chunks from the plan
	chunkIds := make([]uint32, 0)
	for _, cms := range plan {
		chunkIds = append(chunkIds, cms[0].id, cms[1].id)
	}
	a.loadChunks(chunkIds)

	// 3. merge
	removeChunkIds := make([]uint32, 0, len(plan))
	a.dirtyMeta = true
	for _, cms := range plan {
		// update meta
		cm1, cm2 := cms[0], cms[1]
		cm1.size += cm2.size
		cm1.max = cm2.max
		a.meta.Remove(cm2)
		// update chunks
		a.loadedChunks[cm1.id].Add(a.loadedChunks[cm2.id].Items)
		a.dirtyChunks[cm1.id] = struct{}{}
		delete(a.loadedChunks, cm2.id)
		delete(a.dirtyChunks, cm2.id)
		removeChunkIds = append(removeChunkIds, cm2.id)
	}
	err := a.storage.Remove(removeChunkIds)
	if err != nil {
		panic(err)
	}
}

// initMeta loads meta into memory
func (a *SortedArray) initMeta() (err error) {
	if a.metaInit {
		return nil
	}
	a.metaInit = true
	a.meta, err = a.storage.ReadMeta()
	if err != nil {
		return
	}
	a.meta.nextId = 0
	for _, cm := range a.meta.chunks {
		if a.meta.nextId <= cm.id {
			a.meta.nextId = cm.id + 1
		}
	}
	return
}

func NewSortedArray(maxChunkSize uint32, s ChunkStorage) *SortedArray {
	return &SortedArray{
		chunksLock:   sync.Mutex{},
		loadedChunks: make(map[uint32]*Chunk),
		dirtyChunks:  make(map[uint32]struct{}),
		maxChunkSize: maxChunkSize,
		storage:      s,
	}
}
