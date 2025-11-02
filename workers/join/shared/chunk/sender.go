package chunk

import (
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// Sender handles chunk serialization and sending to queues
type Sender struct {
	producer *workerqueue.QueueMiddleware
}

// NewSender creates a new chunk sender
func NewSender(producer *workerqueue.QueueMiddleware) *Sender {
	return &Sender{
		producer: producer,
	}
}

// SendChunk creates, serializes, and sends a chunk
func (s *Sender) SendChunk(
	clientID string,
	fileID string,
	queryType byte,
	chunkNumber int,
	isLastChunk bool,
	isLastFromTable bool,
	chunkSize int,
	tableID int,
	chunkData string,
) middleware.MessageMiddlewareError {
	// Create chunk
	outputChunk := chunk.NewChunk(
		clientID,
		fileID,
		queryType,
		chunkNumber,
		isLastChunk,
		isLastFromTable,
		chunkSize,
		tableID,
		chunkData,
	)

	return s.SendChunkObject(outputChunk)
}

// SendChunkObject serializes and sends a chunk object
func (s *Sender) SendChunkObject(chunkObj *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create chunk message
	chunkMsg := chunk.NewChunkMessage(chunkObj)

	// Serialize
	serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
	if err != nil {
		return middleware.MessageMiddlewareMessageError
	}

	// Send
	return s.producer.Send(serializedData)
}

// SendFromMetadata creates and sends a chunk using metadata from an existing chunk
func (s *Sender) SendFromMetadata(
	sourceChunk *chunk.Chunk,
	chunkNumber int,
	isLastChunk bool,
	chunkSize int,
	chunkData string,
) middleware.MessageMiddlewareError {
	return s.SendChunk(
		sourceChunk.ClientID,
		sourceChunk.FileID,
		sourceChunk.QueryType,
		chunkNumber,
		isLastChunk,
		sourceChunk.IsLastFromTable,
		chunkSize,
		sourceChunk.TableID,
		chunkData,
	)
}
