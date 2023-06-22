package sorted_array

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"testing"
)

func TestInternalChunkingRandomizedTest(t *testing.T) {
	for size := 1; size <= 10; size++ {
		arr := NewSortedArray([]byte("test"), uint32(size), NewInMemoryChunkStorage())
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

//func TestChunkSelection(t *testing.T) {
//	// make sure that adding unsorted values distribute among chunks correctly
//	arr := NewSortedArray([]byte("TERM1"), 2, NewInMemoryChunkStorage())
//	arr.Add([]uint32{10, 20, 30, 40, 50})
//	arr.dumpChunks()
//	arr.Add([]uint32{24, 23, 22, 21})
//	arr.dumpChunks()
//}

func TestChunking(t *testing.T) {
	chunkSize := uint32(3)
	arr := NewSortedArray([]byte("TERM1"), chunkSize, NewInMemoryChunkStorage())

	// Split
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
	arr := NewSortedArray([]byte("TERM1"), chunkSize, NewInMemoryChunkStorage())
	arr.Add([]uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) // 2 chunks
	arr.Delete([]uint32{1, 2, 3, 4, 5, 6, 7, 8})    // remove all except 0, 9
	require.EqualValues(t, [][]uint32{{0, 9}}, arr.getChunks())
}

func TestSimpleAPI(t *testing.T) {
	// one chunk is used here
	arr := NewSortedArray([]byte("TERM1"), 1000, NewInMemoryChunkStorage())

	// Add test
	err := arr.Add([]uint32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	err = arr.Add([]uint32{1, 2, 3, 4, 5}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4, 5}, arr.ToSlice())

	// GetInRange test
	items, err := arr.GetInRange(0, 100) // all
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4, 5}, items)
	items, err = arr.GetInRange(0, 4) // left partial
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4}, items)
	items, err = arr.GetInRange(4, 100) // right partial
	require.NoError(t, err)
	require.EqualValues(t, []uint32{4, 5}, items)
	items, err = arr.GetInRange(1, 2) // inner
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2}, items)

	// Delete
	err = arr.Delete([]uint32{1, 3, 9}) // remove real and absent
	require.NoError(t, err)
	err = arr.Delete([]uint32{1, 3, 9}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{2, 4, 5}, arr.ToSlice())
}
