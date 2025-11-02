package signals

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

// ClientCompletionSignal represents a signal sent by the results dispatcher to the server
// indicating that all queries have been completed for a specific client
type ClientCompletionSignal struct {
	Header   common.Header
	ClientID string
	Message  string
}

// NewClientCompletionSignal creates a new ClientCompletionSignal
func NewClientCompletionSignal(clientID, message string) *ClientCompletionSignal {
	return &ClientCompletionSignal{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ClientCompletionSignalType,
		},
		ClientID: clientID,
		Message:  message,
	}
}

// SerializeClientCompletionSignal serializes a ClientCompletionSignal to bytes
func SerializeClientCompletionSignal(signal *ClientCompletionSignal) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + ClientIDSize + len(signal.Message)
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

	copy(buf[offset:], []byte(signal.Message))

	return buf, nil
}

// DeserializeClientCompletionSignal deserializes bytes to a ClientCompletionSignal
func DeserializeClientCompletionSignal(data []byte) (*ClientCompletionSignal, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize {
		return nil, fmt.Errorf("data too short for ClientCompletionSignal")
	}

	offset := 0

	// Deserialize header
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != common.ClientCompletionSignalType {
		return nil, fmt.Errorf("invalid message type for ClientCompletionSignal: %d", msgTypeID)
	}

	// Deserialize signal data
	clientID := strings.TrimRight(string(data[offset:offset+ClientIDSize]), "\x00")
	offset += ClientIDSize

	// The rest is the message
	message := string(data[offset:])

	return &ClientCompletionSignal{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		ClientID: clientID,
		Message:  message,
	}, nil
}

