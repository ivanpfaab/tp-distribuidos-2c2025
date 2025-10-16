package deserializer

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/common"
	"github.com/tp-distribuidos-2c2025/protocol/signals"
)

// MessageHeader represents the common header structure for all message types
// This is now an alias for Header for backward compatibility
type MessageHeader = common.Header

// Deserialize identifies the message type and deserializes the appropriate message
func Deserialize(data []byte) (interface{}, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize {
		return nil, fmt.Errorf("data too short to contain a valid message header")
	}

	// Get message type
	msgType, err := GetMessageType(data)
	if err != nil {
		return nil, fmt.Errorf("failed to get message type: %w", err)
	}

	// Deserialize based on message type
	switch msgType {
	case common.BatchMessageType:
		return batch.DeserializeBatch(data)
	case common.ChunkMessageType:
		return chunk.DeserializeChunk(data)
	case common.GroupByCompletionSignalType:
		return signals.DeserializeGroupByCompletionSignal(data)
	case common.ChunkNotificationType:
		return signals.DeserializeChunkNotification(data)
	case common.JoinCompletionSignalType:
		return signals.DeserializeJoinCompletionSignal(data)
	case common.JoinCleanupSignalType:
		return signals.DeserializeJoinCleanupSignal(data)
	default:
		return nil, fmt.Errorf("unknown message type: %d", msgType)
	}
}

// GetMessageType returns the message type without full deserialization
func GetMessageType(data []byte) (int, error) {
	if len(data) < common.HeaderLengthSize+common.TotalLengthSize+common.MsgTypeIDSize {
		return 0, fmt.Errorf("data too short to contain message type")
	}

	offset := common.HeaderLengthSize + common.TotalLengthSize
	return int(data[offset]), nil
}

// IsValidMessage checks if the data contains a valid message header
func IsValidMessage(data []byte) bool {
	_, err := Deserialize(data)
	return err == nil
}
