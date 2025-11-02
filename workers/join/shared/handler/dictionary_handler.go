package handler

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/dictionary"
)

// DictionaryHandler processes dictionary messages
type DictionaryHandler[T any] struct {
	manager    *dictionary.Manager[T]
	parseFunc  dictionary.ParseCallback[T]
	workerName string // For logging (e.g., "ItemID Join Worker")
}

// NewDictionaryHandler creates a new dictionary handler
func NewDictionaryHandler[T any](
	manager *dictionary.Manager[T],
	parseFunc dictionary.ParseCallback[T],
	workerName string,
) *DictionaryHandler[T] {
	return &DictionaryHandler[T]{
		manager:    manager,
		parseFunc:  parseFunc,
		workerName: workerName,
	}
}

// ProcessMessage processes a dictionary message
func (dh *DictionaryHandler[T]) ProcessMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("%s: Received dictionary message\n", dh.workerName)

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("%s: Failed to deserialize dictionary chunk: %v\n", dh.workerName, err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	fmt.Printf("%s: Received dictionary message for FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		dh.workerName, chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Load dictionary from CSV
	if err := dh.manager.LoadFromCSV(chunkMsg.ClientID, chunkMsg.ChunkData, dh.parseFunc); err != nil {
		fmt.Printf("%s: Failed to load dictionary from CSV: %v\n", dh.workerName, err)
		delivery.Nack(false, false) // Reject the message
		return middleware.MessageMiddlewareMessageError
	}

	// Check if this is the last chunk for the dictionary
	if chunkMsg.IsLastChunk {
		dh.manager.SetReady(chunkMsg.ClientID)
		fmt.Printf("%s: Received last chunk for FileID: %s - Dictionary is now ready for client %s\n",
			dh.workerName, chunkMsg.FileID, chunkMsg.ClientID)
	}

	delivery.Ack(false) // Acknowledge the dictionary message
	return 0
}
