package chunk

import (
	"encoding/binary"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

const (
	QueryType1 = 1
	QueryType2 = 2
	QueryType3 = 3
	QueryType4 = 4
)

const (
	MessageType     = 2
	ClientIDSize    = 4
	FileIDSize      = 4
	QueryTypeSize   = 1
	TableIDSize     = 1
	ChunkSizeSize   = 8
	ChunkNumberSize = 8
	IsLastChunkSize = 1
	StepSize        = 1
	RetriesSize     = 1
)

type ChunkMessage struct {
	Header common.Header
	Chunk  Chunk
}

func NewChunkMessage(chunk *Chunk) *ChunkMessage {
	return &ChunkMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    MessageType,
		},
		Chunk: *chunk,
	}
}

func SerializeChunkMessage(msg *ChunkMessage) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + ClientIDSize + FileIDSize + QueryTypeSize + TableIDSize + ChunkSizeSize + ChunkNumberSize + IsLastChunkSize + StepSize + RetriesSize

	totalLength := headerLength + len(msg.Chunk.ChunkData)

	buf := make([]byte, totalLength)
	offset := 0

	// Serialize header
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += common.HeaderLengthSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += common.TotalLengthSize

	buf[offset] = byte(msg.Header.MsgTypeID)
	offset += common.MsgTypeIDSize

	// Serialize chunk data
	if len(msg.Chunk.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(msg.Chunk.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(msg.Chunk.ClientID))
	offset += ClientIDSize

	if len(msg.Chunk.FileID) > FileIDSize {
		return nil, fmt.Errorf("file_id too long: %d bytes, max %d", len(msg.Chunk.FileID), FileIDSize)
	}
	copy(buf[offset:], []byte(msg.Chunk.FileID))
	offset += FileIDSize

	buf[offset] = msg.Chunk.QueryType
	offset += QueryTypeSize

	buf[offset] = byte(msg.Chunk.TableID)
	offset += TableIDSize

	binary.BigEndian.PutUint64(buf[offset:], uint64(msg.Chunk.ChunkSize))
	offset += ChunkSizeSize

	binary.BigEndian.PutUint64(buf[offset:], uint64(msg.Chunk.ChunkNumber))
	offset += ChunkNumberSize

	if msg.Chunk.IsLastChunk {
		buf[offset] = 1 // true
	} else {
		buf[offset] = 0 // false
	}
	offset += IsLastChunkSize

	buf[offset] = byte(msg.Chunk.Step)
	offset += StepSize

	buf[offset] = byte(msg.Chunk.Retries)
	offset += RetriesSize

	copy(buf[offset:], []byte(msg.Chunk.ChunkData))

	return buf, nil
}
