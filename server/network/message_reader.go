package network

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
)

const (
	HeaderSize     = 7                // HeaderLength(2) + TotalLength(4) + MsgTypeID(1)
	MaxMessageSize = 10 * 1024 * 1024 // 10MB max message size
)

// MessageHeader represents the parsed message header
type MessageHeader struct {
	HeaderLength uint16
	TotalLength  uint32
	MsgTypeID    uint8
}

// MessageReader handles reading complete messages from TCP connections
type MessageReader struct{}

// NewMessageReader creates a new message reader
func NewMessageReader() *MessageReader {
	return &MessageReader{}
}

// ReadHeader reads and parses the message header from a connection
func (mr *MessageReader) ReadHeader(conn net.Conn) (*MessageHeader, error) {
	headerBuffer := make([]byte, HeaderSize)
	bytesRead := 0

	// Handle TCP short reads by reading until we get all bytes
	for bytesRead < HeaderSize {
		n, err := conn.Read(headerBuffer[bytesRead:])
		if err != nil {
			return nil, fmt.Errorf("failed to read header: %w", err)
		}
		bytesRead += n
	}

	// Parse header components
	headerLength := binary.BigEndian.Uint16(headerBuffer[0:2])
	totalLength := binary.BigEndian.Uint32(headerBuffer[2:6])
	msgTypeID := headerBuffer[6]

	log.Printf("Message Reader: Header - HeaderLength: %d, TotalLength: %d, MsgTypeID: %d",
		headerLength, totalLength, msgTypeID)

	// Basic validation
	if totalLength < HeaderSize || totalLength > MaxMessageSize {
		return nil, fmt.Errorf("invalid total length %d (must be between %d and %d)",
			totalLength, HeaderSize, MaxMessageSize)
	}

	return &MessageHeader{
		HeaderLength: headerLength,
		TotalLength:  totalLength,
		MsgTypeID:    msgTypeID,
	}, nil
}

// ReadMessageBody reads the message body based on header information
func (mr *MessageReader) ReadMessageBody(conn net.Conn, header *MessageHeader) ([]byte, error) {
	// Calculate remaining data size (totalLength includes the header)
	remainingDataSize := int(header.TotalLength) - HeaderSize

	if remainingDataSize < 0 {
		return nil, fmt.Errorf("invalid remaining data size: %d", remainingDataSize)
	}

	// Read the remaining message data - handle TCP short reads
	remainingData := make([]byte, remainingDataSize)
	bytesRead := 0

	for bytesRead < remainingDataSize {
		n, err := conn.Read(remainingData[bytesRead:])
		if err != nil {
			return nil, fmt.Errorf("failed to read message body: %w", err)
		}
		bytesRead += n
	}

	if bytesRead < remainingDataSize {
		return nil, fmt.Errorf("incomplete message body read: %d/%d bytes", bytesRead, remainingDataSize)
	}

	return remainingData, nil
}

// ReadCompleteMessage reads a complete message (header + body) from a connection
func (mr *MessageReader) ReadCompleteMessage(conn net.Conn) ([]byte, error) {
	// Read header
	header, err := mr.ReadHeader(conn)
	if err != nil {
		return nil, err
	}

	// Read header bytes separately to combine with body
	headerBuffer := make([]byte, HeaderSize)
	binary.BigEndian.PutUint16(headerBuffer[0:2], header.HeaderLength)
	binary.BigEndian.PutUint32(headerBuffer[2:6], header.TotalLength)
	headerBuffer[6] = byte(header.MsgTypeID)

	// Read body
	body, err := mr.ReadMessageBody(conn, header)
	if err != nil {
		return nil, err
	}

	// Combine header and body
	completeMessage := append(headerBuffer, body...)

	return completeMessage, nil
}
