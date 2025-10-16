package signals

import (
	"encoding/binary"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

const (
	GroupByCompletionSignalType = 3
	ClientIDSize                = 4
	QueryTypeSize               = 1
	MapWorkerIDSize             = 16
)

// GroupByCompletionSignal represents a signal sent by map workers to reduce workers
// indicating that all data processing has been completed
type GroupByCompletionSignal struct {
	Header      common.Header
	QueryType   int
	ClientID    string
	MapWorkerID string
	Message     string
}

// NewGroupByCompletionSignal creates a new GroupByCompletionSignal
func NewGroupByCompletionSignal(queryType int, clientID, mapWorkerID, message string) *GroupByCompletionSignal {
	return &GroupByCompletionSignal{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    GroupByCompletionSignalType,
		},
		QueryType:   queryType,
		ClientID:    clientID,
		MapWorkerID: mapWorkerID,
		Message:     message,
	}
}

// SerializeGroupByCompletionSignal serializes a GroupByCompletionSignal to bytes
func SerializeGroupByCompletionSignal(signal *GroupByCompletionSignal) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + ClientIDSize + QueryTypeSize + MapWorkerIDSize + len(signal.Message)

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

	buf[offset] = byte(signal.QueryType)
	offset += QueryTypeSize

	if len(signal.MapWorkerID) > MapWorkerIDSize {
		return nil, fmt.Errorf("map_worker_id too long: %d bytes, max %d", len(signal.MapWorkerID), MapWorkerIDSize)
	}
	copy(buf[offset:], []byte(signal.MapWorkerID))
	offset += MapWorkerIDSize

	copy(buf[offset:], []byte(signal.Message))

	return buf, nil
}

// DeserializeGroupByCompletionSignal deserializes bytes to a GroupByCompletionSignal
func DeserializeGroupByCompletionSignal(data []byte) (*GroupByCompletionSignal, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize+QueryTypeSize+MapWorkerIDSize {
		return nil, fmt.Errorf("data too short for GroupByCompletionSignal")
	}

	offset := 0

	// Deserialize header
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != GroupByCompletionSignalType {
		return nil, fmt.Errorf("invalid message type for GroupByCompletionSignal: %d", msgTypeID)
	}

	// Deserialize signal data
	clientID := string(data[offset : offset+ClientIDSize])
	offset += ClientIDSize

	queryType := int(data[offset])
	offset += QueryTypeSize

	mapWorkerID := string(data[offset : offset+MapWorkerIDSize])
	offset += MapWorkerIDSize

	// The rest is the message
	message := string(data[offset:])

	return &GroupByCompletionSignal{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		QueryType:   queryType,
		ClientID:    clientID,
		MapWorkerID: mapWorkerID,
		Message:     message,
	}, nil
}
