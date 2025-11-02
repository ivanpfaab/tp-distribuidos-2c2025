package signals

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

// JoinCleanupSignal represents a signal sent by the garbage collector to join workers
// indicating that cleanup should be performed
type JoinCleanupSignal struct {
	Header   common.Header
	ClientID string
}

// NewJoinCleanupSignal creates a new JoinCleanupSignal
func NewJoinCleanupSignal(clientID string) *JoinCleanupSignal {
	return &JoinCleanupSignal{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.JoinCleanupSignalType,
		},
		ClientID: clientID,
	}
}

// SerializeJoinCleanupSignal serializes a JoinCleanupSignal to bytes
func SerializeJoinCleanupSignal(signal *JoinCleanupSignal) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize +
		ClientIDSize

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

	return buf, nil
}

// DeserializeJoinCleanupSignal deserializes bytes to a JoinCleanupSignal
func DeserializeJoinCleanupSignal(data []byte) (*JoinCleanupSignal, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize {
		return nil, fmt.Errorf("data too short for JoinCleanupSignal")
	}

	offset := 0

	// Deserialize header
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != common.JoinCleanupSignalType {
		return nil, fmt.Errorf("invalid message type for JoinCleanupSignal: %d", msgTypeID)
	}

	// Deserialize signal data
	clientID := strings.TrimRight(string(data[offset:offset+ClientIDSize]), "\x00")
	offset += ClientIDSize

	return &JoinCleanupSignal{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		ClientID: clientID,
	}, nil
}

