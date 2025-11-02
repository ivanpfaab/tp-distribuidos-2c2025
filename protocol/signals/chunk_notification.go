package signals

import (
	"encoding/binary"
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

// ChunkNotification represents a notification from a map worker to the orchestrator
// about chunk processing completion
type ChunkNotification struct {
	Header          common.Header
	ClientID        string
	FileID          string
	TableID         int
	ChunkNumber     int
	IsLastChunk     bool
	IsLastFromTable bool
	MapWorkerID     string
}

// NewChunkNotification creates a new ChunkNotification
func NewChunkNotification(clientID, fileID, mapWorkerID string, tableID, chunkNumber int, isLastChunk, isLastFromTable bool) *ChunkNotification {
	return &ChunkNotification{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ChunkNotificationType,
		},
		ClientID:        clientID,
		FileID:          fileID,
		TableID:         tableID,
		ChunkNumber:     chunkNumber,
		IsLastChunk:     isLastChunk,
		IsLastFromTable: isLastFromTable,
		MapWorkerID:     mapWorkerID,
	}
}

// SerializeChunkNotification serializes a ChunkNotification to bytes
func SerializeChunkNotification(notification *ChunkNotification) ([]byte, error) {
	headerLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize +
		ClientIDSize + FileIDSize + TableIDSize + ChunkNumberSize + 1 + 1 + MapWorkerIDSize // +1 for IsLastChunk, +1 for IsLastFromTable
	totalLength := headerLength

	buf := make([]byte, totalLength)
	offset := 0

	// Header
	binary.BigEndian.PutUint16(buf[offset:], uint16(headerLength))
	offset += common.HeaderLengthSize

	binary.BigEndian.PutUint32(buf[offset:], uint32(totalLength))
	offset += common.TotalLengthSize

	buf[offset] = byte(notification.Header.MsgTypeID)
	offset += common.MsgTypeIDSize

	// ClientID
	if len(notification.ClientID) > ClientIDSize {
		return nil, fmt.Errorf("client_id too long: %d bytes, max %d", len(notification.ClientID), ClientIDSize)
	}
	copy(buf[offset:], []byte(notification.ClientID))
	offset += ClientIDSize

	// FileID
	if len(notification.FileID) > FileIDSize {
		return nil, fmt.Errorf("file_id too long: %d bytes, max %d", len(notification.FileID), FileIDSize)
	}
	copy(buf[offset:], []byte(notification.FileID))
	offset += FileIDSize

	// TableID
	buf[offset] = byte(notification.TableID)
	offset += TableIDSize

	// ChunkNumber
	binary.BigEndian.PutUint32(buf[offset:], uint32(notification.ChunkNumber))
	offset += ChunkNumberSize

	// IsLastChunk
	if notification.IsLastChunk {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset += 1

	// IsLastFromTable
	if notification.IsLastFromTable {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset += 1

	// MapWorkerID
	if len(notification.MapWorkerID) > MapWorkerIDSize {
		return nil, fmt.Errorf("map_worker_id too long: %d bytes, max %d", len(notification.MapWorkerID), MapWorkerIDSize)
	}
	copy(buf[offset:], []byte(notification.MapWorkerID))

	return buf, nil
}

// DeserializeChunkNotification deserializes bytes to a ChunkNotification
func DeserializeChunkNotification(data []byte) (*ChunkNotification, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize+ClientIDSize+FileIDSize+TableIDSize+ChunkNumberSize+1+1+MapWorkerIDSize {
		return nil, fmt.Errorf("data too short for ChunkNotification")
	}

	offset := 0

	// Header
	headerLength := binary.BigEndian.Uint16(data[offset:])
	offset += common.HeaderLengthSize

	totalLength := binary.BigEndian.Uint32(data[offset:])
	offset += common.TotalLengthSize

	msgTypeID := int(data[offset])
	offset += common.MsgTypeIDSize

	if msgTypeID != common.ChunkNotificationType {
		return nil, fmt.Errorf("invalid message type for ChunkNotification: %d", msgTypeID)
	}

	// ClientID
	clientID := string(data[offset : offset+ClientIDSize])
	offset += ClientIDSize

	// FileID
	fileID := string(data[offset : offset+FileIDSize])
	offset += FileIDSize

	// TableID
	tableID := int(data[offset])
	offset += TableIDSize

	// ChunkNumber
	chunkNumber := int(binary.BigEndian.Uint32(data[offset:]))
	offset += ChunkNumberSize

	// IsLastChunk
	isLastChunk := data[offset] == 1
	offset += 1

	// IsLastFromTable
	isLastFromTable := data[offset] == 1
	offset += 1

	// MapWorkerID
	mapWorkerID := string(data[offset : offset+MapWorkerIDSize])

	return &ChunkNotification{
		Header: common.Header{
			HeaderLength: headerLength,
			TotalLength:  int32(totalLength),
			MsgTypeID:    msgTypeID,
		},
		ClientID:        clientID,
		FileID:          fileID,
		TableID:         tableID,
		ChunkNumber:     chunkNumber,
		IsLastChunk:     isLastChunk,
		IsLastFromTable: isLastFromTable,
		MapWorkerID:     mapWorkerID,
	}, nil
}

