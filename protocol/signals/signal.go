package signals

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/common"
)

const (
	GroupByCompletionSignalType = 3
	ChunkNotificationType       = 4
	ClientIDSize                = 4
	QueryTypeSize               = 1
	MapWorkerIDSize             = 32
	FileIDSize                  = 4
	TableIDSize                 = 1
	ChunkNumberSize             = 4
	JoinCompletionSignalType    = 5
	JoinCleanupSignalType       = 6
	ResourceTypeSize            = 32
	ClientCompletionSignalType  = 7
	MessageSize                 = 64
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
			MsgTypeID:    ChunkNotificationType,
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

	if msgTypeID != ChunkNotificationType {
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
			MsgTypeID:    JoinCompletionSignalType,
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

	if msgTypeID != JoinCompletionSignalType {
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
			MsgTypeID:    JoinCleanupSignalType,
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

	if msgTypeID != JoinCleanupSignalType {
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

// ClientCompletionSignal represents a signal sent by the streaming service to the server
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
			MsgTypeID:    ClientCompletionSignalType,
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

	if msgTypeID != ClientCompletionSignalType {
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
