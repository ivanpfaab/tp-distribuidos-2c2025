package batch

import (
	"encoding/binary"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

type Batch struct {
	ClientID        string
	FileID          string
	IsEOF           bool
	IsLastFromTable bool
	BatchNumber     int
	BatchSize       int
	BatchData       string
}

func NewBatch(clientID, fileID string, isEOF, isLastFromTable bool, batchNumber, batchSize int, batchData string) *Batch {
	return &Batch{
		ClientID:        clientID,
		FileID:          fileID,
		IsEOF:           isEOF,
		IsLastFromTable: isLastFromTable,
		BatchNumber:     batchNumber,
		BatchSize:       batchSize,
		BatchData:       batchData,
	}
}

// DeserializeBatch deserializes only the batch body (skips COMMON HEADER, not Batch Message-specific headers)
func DeserializeBatch(data []byte) (*Batch, error) {
	// Skip header (HeaderLength + TotalLength + MsgTypeID)
	offset := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize

	// Read batch fields
	clientIDBytes := data[offset : offset+4]
	clientID := string(clientIDBytes)
	offset += 4

	fileIDBytes := data[offset : offset+4]
	fileID := string(fileIDBytes)
	offset += 4

	isEOF := data[offset] == 1
	offset += 1

	isLastFromTable := data[offset] == 1
	offset += 1

	batchNumber := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	batchSize := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Read batch data
	batchData := ""
	if offset < len(data) {
		batchData = string(data[offset:])
	}

	return &Batch{
		ClientID:        clientID,
		FileID:          fileID,
		IsEOF:           isEOF,
		IsLastFromTable: isLastFromTable,
		BatchNumber:     batchNumber,
		BatchSize:       batchSize,
		BatchData:       batchData,
	}, nil
}
