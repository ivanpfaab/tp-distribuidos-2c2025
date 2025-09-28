package chunk

import (
	"encoding/binary"
	"fmt"
	"tp-distribuidos-2c2025/protocol/common"
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
	QueryTypeSize   = 1
	TableIDSize     = 1
	ChunkSizeSize   = 8
	ChunkNumberSize = 8
	IsLastChunkSize = 1
	StepSize        = 1
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
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + ClientIDSize + QueryTypeSize + TableIDSize + ChunkSizeSize + ChunkNumberSize + IsLastChunkSize + StepSize

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

	copy(buf[offset:], []byte(msg.Chunk.ChunkData))

	return buf, nil
}

func DeserializeChunkMessage(data []byte) (*ChunkMessage, error) {
	offset := 0

	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize+QueryTypeSize+TableIDSize+ChunkSizeSize+ChunkNumberSize+IsLastChunkSize+StepSize {
		return nil, fmt.Errorf("data too short to be a valid ChunkMessage")
	}

	// Read header length
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	// Read total length
	totalLength := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += common.TotalLengthSize

	// Validate total length matches actual data length
	if int32(len(data)) != totalLength {
		return nil, fmt.Errorf("data length (%d) does not match totalLength field (%d)", len(data), totalLength)
	}

	// Read message type
	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	// Read client ID
	clientIDBytes := data[offset : offset+ClientIDSize]
	clientID := string(clientIDBytes)
	offset += ClientIDSize

	// Read query type
	queryType := data[offset]
	offset += QueryTypeSize

	// Read table ID
	tableID := int(data[offset])
	offset += TableIDSize

	// Read chunk size
	chunkSize := int(binary.BigEndian.Uint64(data[offset:]))
	offset += ChunkSizeSize

	// Read chunk number
	chunkNumber := int(binary.BigEndian.Uint64(data[offset:]))
	offset += ChunkNumberSize

	// Read is last chunk
	isLastChunk := false
	if data[offset] == 1 {
		isLastChunk = true
	}
	offset += IsLastChunkSize

	// Read step
	step := int(data[offset])
	offset += StepSize

	// Read chunk data
	chunkData := ""
	if offset < len(data) {
		chunkData = string(data[offset:])
	}

	return &ChunkMessage{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  totalLength,
			MsgTypeID:    msgTypeID,
		},
		Chunk: Chunk{
			ClientID:    clientID,
			QueryType:   queryType,
			TableID:     tableID,
			ChunkSize:   chunkSize,
			ChunkNumber: chunkNumber,
			IsLastChunk: isLastChunk,
			Step:        step,
			ChunkData:   chunkData,
		},
	}, nil
}
