package sorted_array

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"golang.org/x/exp/slices"
)

// Chunk represents an asc sorted array of numbers, grouped together for faster processing
// the type chosen to be uint32 to contain unix timestamps in seconds
// enough for general index purposes when there are not many events per second
type Chunk struct {
	Items []uint32
}

// Add insert new values to the sorted array with just one allocation
func (c *Chunk) Add(items []uint32) {
	newItems := make([]uint32, 0, len(c.Items)+len(items)) // allocate max possible at once
	copy(newItems, c.Items)
	var (
		item  uint32
		pos   int
		found bool
	)
	for _, item = range items {
		pos, found = slices.BinarySearch(newItems, item)
		if found {
			continue // no duplication
		}
		if pos == len(newItems) { // edge-case: append at the end
			newItems = append(newItems, item)
			continue
		}
		copy(newItems[pos+1:], newItems[pos:])
		newItems[pos] = item
	}
	c.Items = newItems
}

func (c *Chunk) Remove(itemsToRemove []uint32) {
	// in-place removal
	for _, removeItem := range itemsToRemove {
		pos, exists := slices.BinarySearch(c.Items, removeItem)
		if !exists {
			continue
		}
		if pos != len(c.Items)-1 {
			copy(c.Items[pos:], c.Items[pos+1:]) // shift
		}
		c.Items = c.Items[:len(c.Items)-1] // reduce size
	}

}

func (c *Chunk) Contains(item uint32) bool { return contains(c.Items, item) }
func (c *Chunk) GetInRange(from, to uint32) []uint32 {
	if from > to {
		panic("from > to")
	}
	retItems := make([]uint32, 0)
	for _, item := range c.Items {
		if item >= from && item <= to {
			retItems = append(retItems, item)
		}
	}
	return retItems
}
func (c *Chunk) Serialize() []byte {
	var serialized bytes.Buffer
	enc := gob.NewEncoder(&serialized)
	err := enc.Encode(c)
	if err != nil {
		panic(fmt.Errorf("unable to encode: %s", err))
	}
	return serialized.Bytes()
}

func NewChunk(items []uint32) *Chunk {
	if items == nil {
		items = make([]uint32, 0)
	}
	return &Chunk{items}
}

func UnserializeChunk(data []byte) *Chunk {
	var c Chunk
	buf := bytes.NewBuffer(data)
	enc := gob.NewDecoder(buf)
	err := enc.Decode(&c)
	if err != nil {
		panic(fmt.Errorf("unable to decode: %s", err))
	}
	return &c
}
