package sorted_array

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func BenchmarkChunkAdd(b *testing.B) {
	chunk := NewChunk(nil)
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		addSlice := make([]uint32, n)
		for i := 0; i < n; i++ {
			addSlice[i] = uint32(i)
		}
		b.StartTimer()
		chunk.Add(addSlice)    // 1 alloc
		chunk.Remove(addSlice) // 0 allocs
	}
	b.ReportAllocs()
}

func TestBasicAPI(t *testing.T) {
	chunk := NewChunk(nil)

	chunk.Add([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	chunk.Add([]uint32{1, 2, 3, 4, 5, 6, 7, 8}) // idempotency
	require.True(t, chunk.Contains(1))
	require.True(t, chunk.Contains(2))
	require.False(t, chunk.Contains(9))

	chunk.Remove([]uint32{1, 8})
	chunk.Remove([]uint32{1, 8}) // idempotency
	require.False(t, chunk.Contains(1))
	require.False(t, chunk.Contains(8))

	slice := chunk.GetInRange(0, 5)
	require.EqualValues(t, []uint32{2, 3, 4, 5}, slice)

	require.EqualValues(t, []uint32{2, 3, 4, 5, 6, 7}, chunk.Items)
}

func TestSerialize(t *testing.T) {
	chunk := NewChunk([]uint32{1, 2, 3})
	s := chunk.Serialize()
	chunk2 := UnserializeChunk(s)
	require.EqualValues(t, chunk.Items, chunk2.Items)
}
