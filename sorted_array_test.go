package sorted_array

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestChunking(t *testing.T) {
	chunkSize := uint32(3)
	arr := NewSortedArray([]byte("TERM1"), chunkSize, NewInMemoryChunkStorage())
	err := arr.Add([]uint32{10, 20, 30, 100, 200}) // 2 chunks should be created
	require.NoError(t, err)
	err = arr.Add([]uint32{10, 20, 30, 100, 200}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{10, 20, 30, 100, 200}, arr.ToSlice())

	// chunks: (10,20,30), (100,200)
	err = arr.Add([]uint32{9, 31, 201}) // add in-between chunks
	require.NoError(t, err)
	require.EqualValues(t, []uint32{9, 10, 20, 30, 31, 100, 200, 201}, arr.ToSlice())
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
