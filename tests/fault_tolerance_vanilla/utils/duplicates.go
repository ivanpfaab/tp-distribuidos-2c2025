package utils

import (
	"math/rand"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// DuplicateSender manages duplicate chunk generation based on a rate
type DuplicateSender struct {
	duplicateRate float64
	sentChunks    map[string][]byte // Map of chunk ID -> serialized chunk
}

// NewDuplicateSender creates a new DuplicateSender
func NewDuplicateSender(duplicateRate float64) *DuplicateSender {
	return &DuplicateSender{
		duplicateRate: duplicateRate,
		sentChunks:    make(map[string][]byte),
	}
}

// ShouldSendDuplicate determines if a duplicate should be sent based on the rate
func (ds *DuplicateSender) ShouldSendDuplicate() bool {
	if ds.duplicateRate <= 0.0 || len(ds.sentChunks) == 0 {
		return false
	}
	return rand.Float64() < ds.duplicateRate
}

// GetRandomDuplicate returns a random previously sent chunk (ID and serialized data)
func (ds *DuplicateSender) GetRandomDuplicate() (string, []byte, bool) {
	if len(ds.sentChunks) == 0 {
		return "", nil, false
	}

	// Get random key from map
	keys := make([]string, 0, len(ds.sentChunks))
	for k := range ds.sentChunks {
		keys = append(keys, k)
	}

	randomID := keys[rand.Intn(len(keys))]
	return randomID, ds.sentChunks[randomID], true
}

// StoreChunk stores a chunk for potential future duplicates
func (ds *DuplicateSender) StoreChunk(chunkMsg *chunk.Chunk, serialized []byte) {
	ds.sentChunks[chunkMsg.ID] = serialized
}

// GetDuplicateCount returns the number of stored chunks available for duplication
func (ds *DuplicateSender) GetDuplicateCount() int {
	return len(ds.sentChunks)
}
