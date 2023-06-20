package sorted_array

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAdd(t *testing.T) {
	index := NewIndex([]byte("TERM1"))
	err := index.Add([]uint32{1, 2, 3, 4, 5})
	require.NoError(t, err)
	err = index.Add([]uint32{1, 2, 3, 4, 5}) // idempotency
	require.NoError(t, err)
	require.EqualValues(t, []uint32{1, 2, 3, 4, 5}, index.ToSlice())
}
