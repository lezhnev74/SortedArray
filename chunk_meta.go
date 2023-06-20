package sorted_array

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/ronanh/intcomp"
	"golang.org/x/exp/slices"
	"sync"
)

// ChunkMeta is a light description of a chunk used to select relevant chunks before fetching them
type ChunkMeta struct {
	id       uint32
	min, max uint32
	size     uint32 // number of items in the chunk
}

func (cm *ChunkMeta) intersects(cm2 *ChunkMeta) bool { return cm.max >= cm2.min && cm.min <= cm2.max }

func (cm *ChunkMeta) contains(item uint32) bool { return item >= cm.min && item <= cm.max }

// Meta contains a list of SORTED chunks descriptions
// No overlapping allowed
type Meta struct {
	nextId uint32
	chunks []*ChunkMeta
}

func NewMeta() *Meta { return &Meta{0, make([]*ChunkMeta, 0)} }

// TakeNextId starts from 0 and returns the NEXT available id
func (m *Meta) TakeNextId() (id uint32) {
	id = m.nextId
	m.nextId++
	return
}

func (m *Meta) Remove(meta *ChunkMeta) {
	pos, exists := findPosForMeta(m.chunks, meta)
	if !exists {
		return
	}
	copy(m.chunks[pos:], m.chunks[pos+1:])
	m.chunks = m.chunks[:len(m.chunks)-1] // collapse after deletion
}

// todo: make this bulk
func (m *Meta) Add(metas []*ChunkMeta) {
	newMeta := make([]*ChunkMeta, len(m.chunks), len(m.chunks)+len(metas))
	copy(newMeta, m.chunks)

	for _, meta := range metas {
		if meta.min > meta.max {
			panic(fmt.Errorf("meta chunk is invalid: %v", meta))
		}

		// find the pos to insert
		pos, exists := findPosForMeta(newMeta, meta)
		if exists {
			panic(fmt.Errorf("trying to add a chunk that intersects with other one:\nchunk:%v\nexisting:%v", meta, m.chunks[pos]))
		}

		// prevent overlapping
		//      +--+   // current
		//   +--+	   // left overlap
		//		 ++    // inner overlap
		//		   +-+ // right overlap
		if pos < len(m.chunks) {
			// the pos is taken by a chunk, check that
			if newMeta[pos].intersects(meta) {
				panic(fmt.Errorf("new chunk intersects with existing.\nnew:%v\nexisting:%v", meta, m.chunks[pos]))
			}
		}
		if pos > 0 {
			// check the previous chunk too
			if newMeta[pos-1].intersects(meta) {
				panic(fmt.Errorf("new chunk intersects with existing.\nnew:%v\nexisting:%v", meta, m.chunks[pos-1]))
			}
		}

		// make insertion
		//copy(newMeta, m.chunks[:pos])
		//copy(newMeta[pos+1:], m.chunks[pos:])
		if pos == len(newMeta) {
			newMeta = append(newMeta, meta)
		} else {
			copy(newMeta[pos+1:], newMeta[pos:])
			newMeta[pos] = meta
		}
	}

	m.chunks = newMeta
}

// findPosForMeta applies binary search to find a position where the chunk SHOULD be
func findPosForMeta(s []*ChunkMeta, item *ChunkMeta) (int, bool) {
	return slices.BinarySearchFunc[*ChunkMeta, *ChunkMeta](s, item, func(a, b *ChunkMeta) int {
		if a.min == b.min {
			return 0
		}
		if a.min < b.min {
			return -1
		}
		return 1
	})
}

// findPosForItem applies binary search to find a position where the item's chunk should be
func (m *Meta) findPosForItem(item uint32) (int, bool) {
	return slices.BinarySearchFunc[*ChunkMeta, uint32](m.chunks, item, func(a *ChunkMeta, i uint32) int {
		if a.contains(i) {
			return 0
		}
		if a.max < item {
			return -1
		}
		return 1
	})
}

// FindRelevantForRead return a link to a chunk description that CAN contains the item
// null means that no chunk CAN contain this item (used in Search)
func (m *Meta) FindRelevantForRead(item uint32) *ChunkMeta {
	pos, found := m.findPosForItem(item)
	if !found {
		return nil
	}
	return m.chunks[pos]
}

// FindRelevantForInsert returns possible chunks that can be used for insertion
// that includes ones that include item, or surround the item
func (m *Meta) FindRelevantForInsert(item uint32) []*ChunkMeta {
	ret := make([]*ChunkMeta, 0, 2)
	pos, found := m.findPosForItem(item)
	if found {
		ret = append(ret, m.chunks[pos])
		return ret
	}

	// not within a chunk, so find relevant surrounding
	if pos > 0 {
		ret = append(ret, m.chunks[pos-1])
	}
	if pos < len(m.chunks) {
		ret = append(ret, m.chunks[pos])
	}

	return ret
}

func (m *Meta) Serialize() ([]byte, error) {
	// A meta is an array of ChunkMeta structures
	// ChunkMeta is a set of numbers: id,size,min,max
	// where min,max are guaranteed sorted, id is likely sorted and size is not sorted
	// The idea is to model them as 4 arrays of numbers and compress them

	wg := sync.WaitGroup{}
	serializedState := make([][]uint32, 5)

	// 0. last id
	serializedState[0] = []uint32{m.nextId}

	// 1. chunks ids
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]uint32, 0, 1000) // split work

		i := 0
		for _, cm := range m.chunks {
			buf = append(buf, cm.id)
			if i%len(buf) == 0 {
				serializedState[1] = intcomp.CompressUint32(buf, serializedState[1])
				i = 0
				buf = buf[:0]
			}
		}
	}()

	// 2. chunks min
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]uint32, 0, 1000) // split work

		i := 0
		for _, cm := range m.chunks {
			buf = append(buf, cm.min)
			if i%len(buf) == 0 {
				serializedState[2] = intcomp.CompressUint32(buf, serializedState[2])
				i = 0
				buf = buf[:0]
			}
		}
	}()

	// 3. chunks max
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]uint32, 0, 1000) // split work

		i := 0
		for _, cm := range m.chunks {
			buf = append(buf, cm.max)
			if i%len(buf) == 0 {
				serializedState[3] = intcomp.CompressUint32(buf, serializedState[3])
				i = 0
				buf = buf[:0]
			}
		}
	}()

	// 4. chunks size
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]uint32, 0, 1000) // split work

		i := 0
		for _, cm := range m.chunks {
			buf = append(buf, cm.size)
			if i%len(buf) == 0 {
				serializedState[4] = intcomp.CompressUint32(buf, serializedState[4])
				i = 0
				buf = buf[:0]
			}
		}
	}()

	wg.Wait()

	// Finally GOB it
	var gobBuf bytes.Buffer
	enc := gob.NewEncoder(&gobBuf)
	err := enc.Encode(serializedState)
	if err != nil {
		return nil, err
	}
	return gobBuf.Bytes(), nil
}

func UnserializeMeta(data []byte) (*Meta, error) {
	gobBuf := bytes.NewBuffer(data)
	enc := gob.NewDecoder(gobBuf)
	serializedState := [][]uint32{
		make([]uint32, 0),
		make([]uint32, 0),
		make([]uint32, 0),
		make([]uint32, 0),
		make([]uint32, 0),
	}
	err := enc.Decode(&serializedState)
	if err != nil {
		return nil, err
	}

	meta := NewMeta()
	meta.nextId = serializedState[0][0]

	var ids, min, max, size []uint32
	wg := sync.WaitGroup{}
	wg.Add(4)

	go func() { defer wg.Done(); ids = intcomp.UncompressUint32(serializedState[1], nil) }()
	go func() { defer wg.Done(); min = intcomp.UncompressUint32(serializedState[2], nil) }()
	go func() { defer wg.Done(); max = intcomp.UncompressUint32(serializedState[3], nil) }()
	go func() { defer wg.Done(); size = intcomp.UncompressUint32(serializedState[4], nil) }()
	wg.Wait()
	meta.chunks = make([]*ChunkMeta, len(ids))

	for i, _ := range ids {
		cm := &ChunkMeta{
			id:   ids[i],
			min:  min[i],
			max:  max[i],
			size: size[i],
		}
		meta.chunks[i] = cm
	}

	return meta, nil
}
