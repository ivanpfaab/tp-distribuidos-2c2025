package signals

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

// JoinCompletionSignal represents a signal sent by join workers to the garbage collector
// indicating that a join operation has been completed
type JoinCompletionSignal struct {
	Header       common.Header
	ClientID     string
	ResourceType string
	WorkerID     string
}

// NewJoinCompletionSignal creates a new JoinCompletionSignal
func NewJoinCompletionSignal(clientID, resourceType, workerID string) *JoinCompletionSignal {
	return &JoinCompletionSignal{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.JoinCompletionSignalType,
		},
		ClientID:     clientID,
		ResourceType: resourceType,
		WorkerID:     workerID,
	}
}

// SerializeJoinCompletionSignal serializes a JoinCompletionSignal to bytes
func SerializeJoinCompletionSignal(signal *JoinCompletionSignal) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize +
		ClientIDSize + ResourceTypeSize + MapWorkerIDSize

	totalLength := headerLength

	buf := make([]byte, totalLength)
	offset := 0

	// Serialize header
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += common.HeaderLengthSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += common.TotalLengthSize

	buf[offset] = byte(signal.Header.MsgTypeID)
	offset += common.MsgTypeIDSize

	// Serialize signal data
	if len(signal.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(signal.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(signal.ClientID))
	offset += ClientIDSize

	if len(signal.ResourceType) > ResourceTypeSize {
		return nil, fmt.Errorf("resource_type too long: %d bytes, max %d", len(signal.ResourceType), ResourceTypeSize)
	}
	copy(buf[offset:], []byte(signal.ResourceType))
	offset += ResourceTypeSize

	if len(signal.WorkerID) > MapWorkerIDSize {
		return nil, fmt.Errorf("worker_id too long: %d bytes, max %d", len(signal.WorkerID), MapWorkerIDSize)
	}
	copy(buf[offset:], []byte(signal.WorkerID))

	return buf, nil
}

// DeserializeJoinCompletionSignal deserializes bytes to a JoinCompletionSignal
func DeserializeJoinCompletionSignal(data []byte) (*JoinCompletionSignal, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize+ResourceTypeSize+MapWorkerIDSize {
		return nil, fmt.Errorf("data too short for JoinCompletionSignal")
	}

	offset := 0

	// Deserialize header
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != common.JoinCompletionSignalType {
		return nil, fmt.Errorf("invalid message type for JoinCompletionSignal: %d", msgTypeID)
	}

	// Deserialize signal data
	clientID := strings.TrimRight(string(data[offset:offset+ClientIDSize]), "\x00")
	offset += ClientIDSize

	resourceType := strings.TrimRight(string(data[offset:offset+ResourceTypeSize]), "\x00")
	offset += ResourceTypeSize

	workerID := strings.TrimRight(string(data[offset:offset+MapWorkerIDSize]), "\x00")

	return &JoinCompletionSignal{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		ClientID:     clientID,
		ResourceType: resourceType,
		WorkerID:     workerID,
	}, nil
}

