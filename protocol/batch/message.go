package batch

import (
	"encoding/binary"
	"fmt"
	"tp-distribuidos-2c2025/protocol/common"
)

const (
	MessageType     = 1
	ClientIDSize    = 4
	FileIDSize      = 4
	IsEOFSize       = 1
	BatchNumberSize = 4
	BatchSizeSize   = 4
)

type BatchMessage struct {
	Header common.Header
	Batch  Batch
}

func NewBatchMessage(batch *Batch) *BatchMessage {
	return &BatchMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    MessageType,
		},
		Batch: *batch,
	}
}

func SerializeBatchMessage(msg *BatchMessage) ([]byte, error) {
	// Calculate header length
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + ClientIDSize + FileIDSize + IsEOFSize + BatchNumberSize + BatchSizeSize

	// Calculate total length
	totalLength := headerLength + len(msg.Batch.BatchData)

	buf := make([]byte, totalLength)
	offset := 0

	// Serialize header
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += common.HeaderLengthSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += common.TotalLengthSize

	buf[offset] = byte(msg.Header.MsgTypeID)
	offset += common.MsgTypeIDSize

	// Serialize batch data
	if len(msg.Batch.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(msg.Batch.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(msg.Batch.ClientID))
	offset += ClientIDSize

	if len(msg.Batch.FileID) > FileIDSize {
		return nil, fmt.Errorf("file_id too long: %d bytes, max %d", len(msg.Batch.FileID), FileIDSize)
	}
	copy(buf[offset:], []byte(msg.Batch.FileID))
	offset += FileIDSize

	if msg.Batch.IsEOF {
		buf[offset] = 1 // true
	} else {
		buf[offset] = 0 // false
	}
	offset += IsEOFSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(msg.Batch.BatchNumber))
	offset += BatchNumberSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(msg.Batch.BatchSize))
	offset += BatchSizeSize

	copy(buf[offset:], []byte(msg.Batch.BatchData))

	return buf, nil
}
