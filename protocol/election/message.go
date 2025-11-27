package election

import (
	"encoding/binary"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

type ElectionMessage struct {
	Header       common.Header
	EventType    ElectionEventType
	SupervisorID int
	Timestamp    int64
}

func NewElectionMessage(eventType ElectionEventType, supervisorID int, timestamp int64) *ElectionMessage {
	return &ElectionMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ElectionMessageType,
		},
		EventType:    eventType,
		SupervisorID: supervisorID,
		Timestamp:    timestamp,
	}
}

func SerializeElectionMessage(msg *ElectionMessage) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize +
		EventTypeSize + SupervisorIDSize + TimestampSize

	totalLength := headerLength
	buf := make([]byte, totalLength)
	offset := 0

	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += common.HeaderLengthSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += common.TotalLengthSize

	buf[offset] = byte(msg.Header.MsgTypeID)
	offset += common.MsgTypeIDSize

	buf[offset] = byte(msg.EventType)
	offset += EventTypeSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(msg.SupervisorID))
	offset += SupervisorIDSize

	binary.BigEndian.PutUint64(buf[offset:], uint64(msg.Timestamp))

	return buf, nil
}

func DeserializeElectionMessage(data []byte) (*ElectionMessage, error) {
	minSize := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize +
		EventTypeSize + SupervisorIDSize + TimestampSize

	if len(data) < minSize {
		return nil, fmt.Errorf("data too short for ElectionMessage: got %d bytes, need at least %d", len(data), minSize)
	}

	offset := 0

	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != common.ElectionMessageType {
		return nil, fmt.Errorf("invalid message type for ElectionMessage: expected %d, got %d", common.ElectionMessageType, msgTypeID)
	}

	eventType := ElectionEventType(data[offset])
	offset += EventTypeSize

	supervisorID := int(binary.BigEndian.Uint32(data[offset:]))
	offset += SupervisorIDSize

	timestamp := int64(binary.BigEndian.Uint64(data[offset:]))

	return &ElectionMessage{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		EventType:    eventType,
		SupervisorID: supervisorID,
		Timestamp:    timestamp,
	}, nil
}

func (m *ElectionMessage) IsElectionStart() bool {
	return m.EventType == ElectionStart
}

func (m *ElectionMessage) IsOk() bool {
	return m.EventType == ElectionOk
}

func (m *ElectionMessage) IsLeader() bool {
	return m.EventType == ElectionLeader
}

