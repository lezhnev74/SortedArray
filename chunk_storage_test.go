package sorted_array

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInMemoryChunkStorage(t *testing.T) {
	storage := NewInMemoryChunkStorage()

	// Read:
	chunks, _ := storage.Read([]int{1, 2})
	require.Equal(t, 2, len(chunks))
	require.Nil(t, chunks[1])
	require.Nil(t, chunks[2])

	// Write:
	chunks[1] = NewChunk([]uint32{100, 200})
	chunks[2] = NewChunk([]uint32{300, 400})
	storage.Save(chunks)

	chunks2, _ := storage.Read([]int{1, 2})
	require.Equal(t, 2, len(chunks2))
	require.EqualValues(t, chunks, chunks2)

	// Remove:
	storage.Remove([]int{1})
	chunks3, _ := storage.Read([]int{1, 2})
	require.Equal(t, 2, len(chunks3))
	require.Nil(t, chunks3[1])
	require.EqualValues(t, chunks[2], chunks3[2])

	// Meta:
	list := []ChunkMeta{
		{1, 0, 2, 2},
		{2, 3, 4, 2},
	}
	storage.SaveMeta(list)
	list2, _ := storage.ReadMeta()
	require.EqualValues(t, list, list2)
}
