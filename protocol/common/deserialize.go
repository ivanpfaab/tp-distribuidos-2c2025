package common

import (
	"encoding/binary"
	"fmt"
)

// MessageHeader represents the common header structure for all message types
// This is now an alias for Header for backward compatibility
type MessageHeader = Header

// Batch represents batch data structure (duplicated to avoid circular dependency)
type Batch struct {
	ClientID    string
	FileID      string
	IsEOF       bool
	BatchNumber int
	BatchSize   int
	BatchData   string
}

// Deserialize identifies the message type and deserializes the appropriate message
func Deserialize(data []byte) (interface{}, error) {
	if len(data) < HeaderLengthSize+TotalLengthSize+MsgTypeIDSize {
		return nil, fmt.Errorf("data too short to contain a valid message header")
	}

	// Get message type
	msgType, err := GetMessageType(data)
	if err != nil {
		return nil, fmt.Errorf("failed to get message type: %w", err)
	}

	// Deserialize based on message type
	switch msgType {
	case BatchMessageType:
		return DeserializeBatch(data)
	case ChunkMessageType:
		// TODO: Implement chunk message deserialization
		return nil, fmt.Errorf("chunk messages not supported in this version")
	default:
		return nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

// GetMessageType returns the message type without full deserialization
func GetMessageType(data []byte) (int, error) {
	if len(data) < HeaderLengthSize+TotalLengthSize+MsgTypeIDSize {
		return 0, fmt.Errorf("data too short to contain message type")
	}

	offset := HeaderLengthSize + TotalLengthSize
	return int(data[offset]), nil
}

// DeserializeBatch deserializes only the batch body (skips COMMON HEADER, not Batch Message-specific headers)
func DeserializeBatch(data []byte) (*Batch, error) {
	// Skip header (HeaderLength + TotalLength + MsgTypeID)
	offset := HeaderLengthSize + TotalLengthSize + MsgTypeIDSize

	// Skip to batch data fields
	offset += 4 // ClientID
	offset += 4 // FileID
	offset += 1 // IsEOF
	offset += 4 // BatchNumber
	offset += 4 // BatchSize

	// Read batch data
	batchData := ""
	if offset < len(data) {
		batchData = string(data[offset:])
	}

	// Read batch fields
	clientIDBytes := data[HeaderLengthSize+TotalLengthSize+MsgTypeIDSize : HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+4]
	clientID := string(clientIDBytes)

	fileIDBytes := data[HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+4 : HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+8]
	fileID := string(fileIDBytes)

	isEOF := data[HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+8] == 1

	batchNumber := int(binary.BigEndian.Uint32(data[HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+9:]))
	batchSize := int(binary.BigEndian.Uint32(data[HeaderLengthSize+TotalLengthSize+MsgTypeIDSize+13:]))

	return &Batch{
		ClientID:    clientID,
		FileID:      fileID,
		IsEOF:       isEOF,
		BatchNumber: batchNumber,
		BatchSize:   batchSize,
		BatchData:   batchData,
	}, nil
}

// IsValidMessage checks if the data contains a valid message header
func IsValidMessage(data []byte) bool {
	_, err := Deserialize(data)
	return err == nil
}
