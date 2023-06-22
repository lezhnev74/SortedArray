package sorted_array

import (
	"fmt"
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

	added := chunk.Add([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	require.Equal(t, 8, added)
	added = chunk.Add([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9})
	require.Equal(t, 1, added)
	added = chunk.Add([]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9}) // idempotency
	require.Equal(t, 0, added)
	require.True(t, chunk.Contains(1))
	require.True(t, chunk.Contains(2))
	require.False(t, chunk.Contains(99))

	chunk.Remove([]uint32{1, 9})
	chunk.Remove([]uint32{1, 9}) // idempotency
	require.False(t, chunk.Contains(1))
	require.False(t, chunk.Contains(9))

	slice := chunk.GetInRange(0, 5)
	require.EqualValues(t, []uint32{2, 3, 4, 5}, slice)

	require.EqualValues(t, []uint32{2, 3, 4, 5, 6, 7, 8}, chunk.Items)
}

func TestAdd(t *testing.T) {
	type test struct {
		existingItems, addItems []uint32
		expectedAdded           int
		expectedSlice           []uint32
	}
	tests := []test{
		{ // add left
			[]uint32{10, 20, 30},
			[]uint32{9},
			1,
			[]uint32{9, 10, 20, 30},
		},
		{ // add right
			[]uint32{10, 20, 30},
			[]uint32{40},
			1,
			[]uint32{10, 20, 30, 40},
		},
		{ // add middle
			[]uint32{10, 20, 30},
			[]uint32{15, 25},
			2,
			[]uint32{10, 15, 20, 25, 30},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			chunk := NewChunk(tt.existingItems)
			added := chunk.Add(tt.addItems)
			require.Equal(t, tt.expectedAdded, added)
			added = chunk.Add(tt.addItems)
			require.Equal(t, 0, added) // idempotency check
			require.EqualValues(t, tt.expectedSlice, chunk.Items)

			// contains check
			for _, item := range tt.addItems {
				require.True(t, chunk.Contains(item))
			}
		})
	}
}

func TestRemove(t *testing.T) {
	type test struct {
		existingItems, removeItems []uint32
		expectedRemoved            int
		expectedSlice              []uint32
	}
	tests := []test{
		{ // no-op
			[]uint32{10, 20, 30},
			[]uint32{9},
			0,
			[]uint32{10, 20, 30},
		},
		{ // remove left
			[]uint32{10, 20, 30},
			[]uint32{10},
			1,
			[]uint32{20, 30},
		},
		{ // remove right
			[]uint32{10, 20, 30},
			[]uint32{30},
			1,
			[]uint32{10, 20},
		},
		{ // remove middle
			[]uint32{10, 20, 30},
			[]uint32{20},
			1,
			[]uint32{10, 30},
		},
		{ // remove middle
			[]uint32{10, 20, 21, 22, 30},
			[]uint32{20, 22},
			2,
			[]uint32{10, 21, 30},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			chunk := NewChunk(tt.existingItems)
			added := chunk.Remove(tt.removeItems)
			require.Equal(t, tt.expectedRemoved, added)
			added = chunk.Remove(tt.removeItems) // idempotency
			require.Equal(t, 0, added)
			require.EqualValues(t, tt.expectedSlice, chunk.Items)

			// contains check
			for _, item := range tt.removeItems {
				require.False(t, chunk.Contains(item))
			}
		})
	}
}

func TestSerialize(t *testing.T) {
	chunk := NewChunk([]uint32{1, 2, 3})
	s := chunk.Serialize()
	chunk2 := UnserializeChunk(s)
	require.EqualValues(t, chunk.Items, chunk2.Items)
}
