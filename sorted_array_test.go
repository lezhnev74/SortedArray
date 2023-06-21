package sorted_array

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSimpleAPI(t *testing.T) {
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
}
