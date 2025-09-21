package batch

import (
	"encoding/binary"
	"fmt"
)

// Field size constants
const (
	BatchMessageType = 1
	HeaderLengthSize = 2
	TotalLengthSize  = 4
	MsgTypeIDSize    = 1
	ClientIDSize     = 4
	FileIDSize       = 4
	IsEOFSize        = 1
	BatchNumberSize  = 4
	BatchSizeSize    = 4
)

// BatchMessage represents a message containing batch data
type BatchMessage struct {
	HeaderLength uint16
	TotalLength  int32
	MsgTypeID    int
	ClientID     string
	FileID       string
	IsEOF        bool
	BatchNumber  int
	BatchSize    int
	BatchData    string
}

// NewBatchMessage creates a new BatchMessage from a Batch
func NewBatchMessage(batch *Batch) *BatchMessage {
	return &BatchMessage{
		HeaderLength: 0, 
		TotalLength:  0, 
		MsgTypeID:    BatchMessageType,
		ClientID:     batch.ClientID,
		FileID:       batch.FileID,
		IsEOF:        batch.IsEOF,
		BatchNumber:  batch.BatchNumber,
		BatchSize:    batch.BatchSize,
		BatchData:    batch.BatchData,
	}
}

// SerializeBatchMessage serializes a BatchMessage to bytes
func SerializeBatchMessage(msg *BatchMessage) ([]byte, error) {
	// Calculate header length
	headerLength := HeaderLengthSize + TotalLengthSize + MsgTypeIDSize + ClientIDSize + FileIDSize + IsEOFSize + BatchNumberSize + BatchSizeSize
	
	// Calculate total length
	totalLength := headerLength + len(msg.BatchData)
	
	// Create buffer
	buf := make([]byte, totalLength)
	offset := 0
	
	// Write header_length
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += HeaderLengthSize
	
	// Write total_length
	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += TotalLengthSize
	
	// Write msg_type_id
	buf[offset] = byte(msg.MsgTypeID)
	offset += MsgTypeIDSize
	
	// Write client_id (assuming fixed length string)
	if len(msg.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(msg.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(msg.ClientID))
	offset += ClientIDSize
	
	// Write file_id (assuming fixed length string)
	if len(msg.FileID) > FileIDSize {
		return nil, fmt.Errorf("file_id too long: %d bytes, max %d", len(msg.FileID), FileIDSize)
	}
	copy(buf[offset:], []byte(msg.FileID))
	offset += FileIDSize
	
	// Write is_eof
	if msg.IsEOF {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset += IsEOFSize
	
	// Write batch_number
	binary.BigEndian.PutUint32(buf[offset:], uint32(msg.BatchNumber))
	offset += BatchNumberSize
	
	// Write batch_size
	binary.BigEndian.PutUint32(buf[offset:], uint32(msg.BatchSize))
	offset += BatchSizeSize
	
	// Write batch_data (variable length)
	copy(buf[offset:], []byte(msg.BatchData))
	
	return buf, nil
}
