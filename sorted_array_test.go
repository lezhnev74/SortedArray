package sorted_array

import (
	sorted_numeric_streams "github.com/lezhnev74/SetOperationsOnSortedNumericStreams"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"testing"
)

func TestStorageIntegration(t *testing.T) {
	storage := NewInMemoryChunkStorage()

	// Make modifications from instance1
	arr1 := NewSortedArray(3, storage)
	arr1.Add([]uint32{10, 20, 30, 40, 50})
	arr1.Delete([]uint32{10, 30, 50})
	require.EqualValues(t, []uint32{20, 40}, arr1.ToSlice())
	arr1.Flush()

	// Read from instance2, modify
	arr2 := NewSortedArray(10, storage)
	require.EqualValues(t, []uint32{20, 40}, arr2.ToSlice())
	arr2.Add([]uint32{1, 30, 99})
	arr2.Delete([]uint32{40})
	require.EqualValues(t, []uint32{1, 20, 30, 99}, arr2.ToSlice())
	arr2.Flush()
}

func TestInternalChunkingRandomizedTest(t *testing.T) {
	for size := 1; size <= 10; size++ {
		arr := NewSortedArray(uint32(size), NewInMemoryChunkStorage())
		for i := 0; i < 1000; i++ {
			items := make([]uint32, 0)
			// add
			insertOps := rand.Int() % 100
			for j := 0; j < insertOps; j++ {
				items = append(items, uint32(rand.Int()%1000))
			}
			arr.Add(items)
			// remove
			items = items[:0]
			removeOps := rand.Int() % 100
			for j := 0; j < removeOps; j++ {
				items = append(items, uint32(rand.Int()%1000))
			}
			arr.Delete(items)
		}
	}
}

func TestChunking(t *testing.T) {
	chunkSize := uint32(3)
	arr := NewSortedArray(chunkSize, NewInMemoryChunkStorage())

	// split
	err := arr.Add([]uint32{10, 20, 30, 100, 200}) // 2 chunks should be created
	require.NoError(t, err)
	err = arr.Add([]uint32{10, 20, 30, 100, 200}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{10, 20, 30, 100, 200}, arr.ToSlice())

	// chunks: (10,20,30), (100,200)
	err = arr.Add([]uint32{9, 31, 201}) // add in-between chunks
	require.NoError(t, err)
	require.EqualValues(t, []uint32{9, 10, 20, 30, 31, 100, 200, 201}, arr.ToSlice())

	// Remove
	err = arr.Delete([]uint32{10, 20, 30, 200})
	require.NoError(t, err)
	require.EqualValues(t, []uint32{9, 31, 100, 201}, arr.ToSlice())
}

func TestMerge(t *testing.T) {
	chunkSize := uint32(5)
	arr := NewSortedArray(chunkSize, NewInMemoryChunkStorage())
	arr.Add([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) // 2 chunks
	arr.Delete([]uint32{1, 2, 3, 4, 5, 6, 7, 8})    // remove all except 0, 9
	require.EqualValues(t, [][]uint32{{0, 9}}, arr.getChunks())
}

func TestSimpleAPI(t *testing.T) {
	// one chunk is used here
	arr := NewSortedArray(1000, NewInMemoryChunkStorage())

	// Add test
	err := arr.Add([]uint32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	err = arr.Add([]uint32{1, 2, 3, 4, 5}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4, 5}, arr.ToSlice())

	// GetInRange test
	items, err := arr.GetInRange(0, 100) // all
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4, 5}, sorted_numeric_streams.ToSlice(items))
	items, err = arr.GetInRange(0, 4) // left partial
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4}, sorted_numeric_streams.ToSlice(items))
	items, err = arr.GetInRange(4, 100) // right partial
	require.NoError(t, err)
	require.EqualValues(t, []uint32{4, 5}, sorted_numeric_streams.ToSlice(items))
	items, err = arr.GetInRange(1, 2) // inner
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2}, sorted_numeric_streams.ToSlice(items))

	// Delete
	err = arr.Delete([]uint32{1, 3, 9}) // remove real and absent
	require.NoError(t, err)
	err = arr.Delete([]uint32{1, 3, 9}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{2, 4, 5}, arr.ToSlice())
}
