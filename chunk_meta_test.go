package sorted_array

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMetaBasicAPI(t *testing.T) {
	meta := NewMeta()
	nextId := meta.TakeNextId()
	require.EqualValues(t, 0, nextId)

	c1 := &ChunkMeta{nextId, 0, 1, 1}
	c2 := &ChunkMeta{nextId, 999, 1111, 1}
	meta.Add([]*ChunkMeta{c1})
	require.EqualValues(t, 1, meta.TakeNextId())

	meta.Remove(c1)
	meta.Remove(c2) // non existent, a no-op
	require.Len(t, meta.chunks, 0)
}

func TestAddBulk(t *testing.T) {
	meta := NewMeta()
	nextId := meta.TakeNextId()
	require.EqualValues(t, 0, nextId)

	// First add
	c1 := &ChunkMeta{nextId, 10, 15, 1}
	c2 := &ChunkMeta{nextId, 20, 25, 1}
	meta.Add([]*ChunkMeta{c1, c2})
	require.EqualValues(t, []*ChunkMeta{c1, c2}, meta.chunks)

	// Add items to before, in the middle and after existing chunks
	c3 := &ChunkMeta{nextId, 0, 1, 1}   // before
	c4 := &ChunkMeta{nextId, 16, 17, 1} // in the middle
	c5 := &ChunkMeta{nextId, 30, 31, 1} // after
	meta.Add([]*ChunkMeta{c3, c4, c5})
	require.EqualValues(t, []*ChunkMeta{c3, c1, c4, c2, c5}, meta.chunks)
}

func TestMetaOverlapProtection(t *testing.T) {
	meta := NewMeta()
	meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 2, 4, 2}})
	require.Panics(t, func() { meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 2, 4, 2}}) }) // exact match
	require.Panics(t, func() { meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 1, 2, 2}}) }) // left overlap
	require.Panics(t, func() { meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 3, 3, 2}}) }) // middle overlap
	require.Panics(t, func() { meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 4, 6, 2}}) }) // right overlap
	require.Panics(t, func() { meta.Add([]*ChunkMeta{&ChunkMeta{meta.TakeNextId(), 9, 0, 1}}) }) // min-max violation
}

func TestMetaSearchRelevantForRead(t *testing.T) {
	meta := NewMeta()
	chunk1 := ChunkMeta{meta.TakeNextId(), 10, 15, 2}
	chunk2 := ChunkMeta{meta.TakeNextId(), 20, 25, 2}
	meta.Add([]*ChunkMeta{&chunk1})
	meta.Add([]*ChunkMeta{&chunk2})

	relevantChunk := meta.FindRelevantForRead(1)
	require.Nil(t, relevantChunk)
	relevantChunk = meta.FindRelevantForRead(17)
	require.Nil(t, relevantChunk)
	relevantChunk = meta.FindRelevantForRead(30)
	require.Nil(t, relevantChunk)

	relevantChunk = meta.FindRelevantForRead(24)
	require.EqualValues(t, chunk2, *relevantChunk)
}

func TestMetaSearchRelevantForInsert(t *testing.T) {
	meta := NewMeta()

	relevantChunks := meta.FindRelevantForInsert(1) // no chunks in an empty meta
	require.Len(t, relevantChunks, 0)

	// put some initial state
	chunk1 := ChunkMeta{meta.TakeNextId(), 10, 15, 2}
	chunk2 := ChunkMeta{meta.TakeNextId(), 20, 25, 2}
	meta.Add([]*ChunkMeta{&chunk1})
	meta.Add([]*ChunkMeta{&chunk2})

	relevantChunks = meta.FindRelevantForInsert(13) // item within one chunk
	require.EqualValues(t, []*ChunkMeta{&chunk1}, relevantChunks)

	relevantChunks = meta.FindRelevantForInsert(1) // item before any chunk
	require.EqualValues(t, []*ChunkMeta{&chunk1}, relevantChunks)

	relevantChunks = meta.FindRelevantForInsert(17) // item in between chunks
	require.EqualValues(t, []*ChunkMeta{&chunk1, &chunk2}, relevantChunks)

	relevantChunks = meta.FindRelevantForInsert(30) // item after any chunk
	require.EqualValues(t, []*ChunkMeta{&chunk2}, relevantChunks)
}

func TestSerialization(t *testing.T) {

	type test struct {
		items int
	}
	tests := []test{
		{1},
		{2},
		{10},
		{100},
		{1_000_000},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d", tt.items), func(t *testing.T) {
			meta := NewMeta()
			chunks := make([]*ChunkMeta, 0, tt.items)
			for i := 0; i < tt.items; i++ {
				chunks = append(chunks, &ChunkMeta{meta.TakeNextId(), uint32(i * 10), uint32(i*10 + 5), 5})
			}
			meta.Add(chunks)

			b, err := meta.Serialize()
			require.NoError(t, err)
			meta2, err := UnserializeMeta(b)
			require.NoError(t, err)
			require.EqualValues(t, meta, meta2)
		})
	}
}

func BenchmarkSerialization(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	meta := NewMeta()
	chunks := make([]*ChunkMeta, 0, b.N)
	for i := 0; i < b.N; i++ {
		chunks = append(chunks, &ChunkMeta{meta.TakeNextId(), uint32(i * 10), uint32(i*10 + 5), 5})
	}
	meta.Add(chunks)

	b.StartTimer()
	s, err := meta.Serialize()
	require.NoError(b, err)
	meta2, err := UnserializeMeta(s)
	require.NoError(b, err)
	require.EqualValues(b, meta, meta2)
}
