package chunk

import (
	"encoding/binary"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

// ensureSize ensures a string is exactly the specified size by padding with spaces or truncating
func ensureSize(s string, size int) string {
	if len(s) > size {
		return s[:size]
	}
	if len(s) < size {
		// Pad with spaces
		return fmt.Sprintf("%-*s", size, s)
	}
	return s
}

type Chunk struct {
	ClientID        string
	FileID          string
	ID              string
	QueryType       byte
	TableID         int
	ChunkSize       int
	ChunkNumber     int
	IsLastChunk     bool
	IsLastFromTable bool
	ChunkData       string
}

func NewChunk(clientID, fileID string, queryType byte, chunkNumber int, isLastChunk, isLastFromTable bool, chunkSize, tableID int, chunkData string) *Chunk {
	// Generate ID: ClientID (4 bytes) + FileID (4 bytes) + QueryType (1 byte) + ChunkNumber (8 bytes as uint64)
	// Ensure each component has the correct size
	clientIDFixed := ensureSize(clientID, ClientIDSize)
	fileIDFixed := ensureSize(fileID, FileIDSize)
	queryTypeStr := fmt.Sprintf("%d", queryType)               // QueryType is 1 byte (single digit 1-4)
	chunkNumberStr := fmt.Sprintf("%08d", uint64(chunkNumber)) // ChunkNumber is 8 bytes (8-digit zero-padded)
	id := clientIDFixed + fileIDFixed + queryTypeStr + chunkNumberStr

	return &Chunk{
		ClientID:        clientID,
		FileID:          fileID,
		ID:              id,
		QueryType:       queryType,
		ChunkNumber:     chunkNumber,
		IsLastChunk:     isLastChunk,
		IsLastFromTable: isLastFromTable,
		ChunkSize:       chunkSize,
		TableID:         tableID,
		ChunkData:       chunkData,
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

	fileIDBytes := data[offset : offset+FileIDSize]
	fileID := string(fileIDBytes)
	offset += FileIDSize

	// Read ID (17 bytes: ClientID + FileID + QueryType + ChunkNumber)
	idBytes := data[offset : offset+IDSize]
	id := string(idBytes)
	offset += IDSize

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

	isLastFromTable := data[offset] == 1
	offset += IsLastFromTableSize

	// Read chunk data
	chunkData := ""
	if offset < len(data) {
		chunkData = string(data[offset:])
	}

	return &Chunk{
		ClientID:        clientID,
		FileID:          fileID,
		ID:              id,
		QueryType:       queryType,
		TableID:         tableID,
		ChunkSize:       chunkSize,
		ChunkNumber:     chunkNumber,
		IsLastChunk:     isLastChunk,
		IsLastFromTable: isLastFromTable,
		ChunkData:       chunkData,
	}, nil
}
