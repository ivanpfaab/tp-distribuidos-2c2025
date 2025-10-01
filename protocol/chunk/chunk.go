package chunk

import (
	"encoding/binary"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

type Chunk struct {
	ClientID    string
	QueryType   byte
	TableID     int
	ChunkSize   int
	ChunkNumber int
	IsLastChunk bool
	Step        int
	ChunkData   string
}

func NewChunk(clientID string, queryType byte, chunkNumber int, isLastChunk bool, step, chunkSize, tableID int, chunkData string) *Chunk {
	return &Chunk{
		ClientID:    clientID,
		QueryType:   queryType,
		ChunkNumber: chunkNumber,
		IsLastChunk: isLastChunk,
		Step:        step,
		ChunkSize:   chunkSize,
		TableID:     tableID,
		ChunkData:   chunkData,
	}
}

// DeserializeChunk deserializes only the chunk body (skips COMMON HEADER, not Chunk Message-specific headers)
func DeserializeChunk(data []byte) (*Chunk, error) {
	// Skip header (HeaderLength + TotalLength + MsgTypeID)
	offset := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize

	// Read chunk fields
	clientIDBytes := data[offset : offset+ClientIDSize]
	clientID := string(clientIDBytes)
	offset += ClientIDSize

	queryType := data[offset]
	offset += QueryTypeSize

	tableID := int(data[offset])
	offset += TableIDSize

	chunkSize := int(binary.BigEndian.Uint64(data[offset:]))
	offset += ChunkSizeSize

	chunkNumber := int(binary.BigEndian.Uint64(data[offset:]))
	offset += ChunkNumberSize

	isLastChunk := data[offset] == 1
	offset += IsLastChunkSize

	step := int(data[offset])
	offset += StepSize

	// Read chunk data
	chunkData := ""
	if offset < len(data) {
		chunkData = string(data[offset:])
	}

	return &Chunk{
		ClientID:    clientID,
		QueryType:   queryType,
		TableID:     tableID,
		ChunkSize:   chunkSize,
		ChunkNumber: chunkNumber,
		IsLastChunk: isLastChunk,
		Step:        step,
		ChunkData:   chunkData,
	}, nil
}

func AdvanceChunkStep(chunk *Chunk) *Chunk {
	chunk.Step++
	return chunk
}
