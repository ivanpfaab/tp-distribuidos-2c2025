package common

// Header represents the common header structure for all message types
type Header struct {
	HeaderLength uint16
	TotalLength  int32
	MsgTypeID    int
}

// MessageType constants
const (
	BatchMessageType            = 1
	ChunkMessageType            = 2
	GroupByCompletionSignalType = 3
)

// Common header sizes
const (
	HeaderLengthSize = 2
	TotalLengthSize  = 4
	MsgTypeIDSize    = 1
)
