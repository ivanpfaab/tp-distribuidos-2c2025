package chunk

import (
	"encoding/binary"
	"fmt"
)

const (
	QueryType1 = 1
	QueryType2 = 2
	QueryType3 = 3
	QueryType4 = 4
)

const (
	ChunkMessageType = 2
	HeaderLengthSize = 2
	TotalLengthSize  = 4
	MsgTypeIDSize    = 1
	ClientIDSize     = 4
	QueryTypeSize    = 1
	TableIDSize      = 1
	ChunkSizeSize    = 8
	ChunkNumberSize  = 8
	IsLastChunkSize  = 1
	StepSize         = 1
)

type ChunkMessage struct {
	HeaderLength uint16
	TotalLength  int32
	MsgTypeID    int
	ClientID     string
	QueryType    uint8
	ChunkNumber  int
	IsLastChunk  bool
	Step         int
	ChunkSize    int
	TableID      int
	ChunkData    string
}

func NewChunkMessage(chunk *Chunk) *ChunkMessage {
	return &ChunkMessage{
		HeaderLength: 0, 
		TotalLength:  0, 
		MsgTypeID:    ChunkMessageType,
		ClientID:     chunk.ClientID,
		QueryType:    chunk.QueryType,
		ChunkNumber:  chunk.ChunkNumber,
		IsLastChunk:  chunk.IsLastChunk,
		Step:         chunk.Step,
		ChunkSize:    chunk.ChunkSize,
		TableID:      chunk.TableID,
		ChunkData:    chunk.ChunkData,
	}
}

func SerializeChunkMessage(msg *ChunkMessage) ([]byte, error) {
	headerLength := HeaderLengthSize + TotalLengthSize + MsgTypeIDSize + ClientIDSize + QueryTypeSize + TableIDSize + ChunkSizeSize + ChunkNumberSize + IsLastChunkSize + StepSize
	
	totalLength := headerLength + len(msg.ChunkData)
	
	buf := make([]byte, totalLength)
	offset := 0
	
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += HeaderLengthSize
	
	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += TotalLengthSize
	
	buf[offset] = byte(msg.MsgTypeID)
	offset += MsgTypeIDSize
	
	if len(msg.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(msg.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(msg.ClientID))
	offset += ClientIDSize
	
	buf[offset] = msg.QueryType
	offset += QueryTypeSize
	
	buf[offset] = byte(msg.TableID)
	offset += TableIDSize
	
	binary.BigEndian.PutUint64(buf[offset:], uint64(msg.ChunkSize))
	offset += ChunkSizeSize
	
	binary.BigEndian.PutUint64(buf[offset:], uint64(msg.ChunkNumber))
	offset += ChunkNumberSize
	
	if msg.IsLastChunk {
		buf[offset] = 1 // true
	} else {
		buf[offset] = 0 // false
	}
	offset += IsLastChunkSize
	
	buf[offset] = byte(msg.Step)
	offset += StepSize
	
	copy(buf[offset:], []byte(msg.ChunkData))
	
	return buf, nil
}

func DeserializeChunkMessage(data []byte) (*ChunkMessage, error) {
	offset := 0

	if len(data) < HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+ClientIDSize+QueryTypeSize+TableIDSize+ChunkSizeSize+ChunkNumberSize+IsLastChunkSize+StepSize {
		return nil, fmt.Errorf("data too short to be a valid ChunkMessage")
	}

	//TODO: Check the header to guarantee the integrity of the message
	//headerLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += HeaderLengthSize

	totalLength := int(binary.BigEndian.Uint32(data[offset:]))
	offset += TotalLengthSize

	if len(data) != totalLength {
		return nil, fmt.Errorf("data length (%d) does not match totalLength field (%d)", len(data), totalLength)
	}

	msgTypeID := uint8(data[offset])
	offset += MsgTypeIDSize

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

	isLastChunk := false
	if data[offset] == 1 {
		isLastChunk = true
	}
	offset += IsLastChunkSize

	step := int(data[offset])
	offset += StepSize

	chunkData := ""
	if offset < len(data) {
		chunkData = string(data[offset:])
	}

	return &ChunkMessage{
		MsgTypeID:   int(msgTypeID),
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