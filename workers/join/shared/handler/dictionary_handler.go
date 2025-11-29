package handler

import (
	"fmt"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/workers/join/shared/dictionary"
)

// DictionaryHandler processes dictionary messages
type DictionaryHandler[T any] struct {
	manager    *dictionary.Manager[T]
	parseFunc  dictionary.ParseCallback[T]
	dictDir    string
	workerName string // For logging (e.g., "ItemID Join Worker")
}

// NewDictionaryHandler creates a new dictionary handler
func NewDictionaryHandler[T any](
	manager *dictionary.Manager[T],
	parseFunc dictionary.ParseCallback[T],
	dictDir string,
	workerName string,
) *DictionaryHandler[T] {
	return &DictionaryHandler[T]{
		manager:    manager,
		parseFunc: parseFunc,
		dictDir:    dictDir,
		workerName: workerName,
	}
}

// ProcessMessage processes a dictionary message
func (dh *DictionaryHandler[T]) ProcessMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	fmt.Printf("%s: Received dictionary message\n", dh.workerName)

	fmt.Printf("%s: Received dictionary message for FileID: %s, ChunkNumber: %d, IsLastChunk: %t\n",
		dh.workerName, chunkMsg.FileID, chunkMsg.ChunkNumber, chunkMsg.IsLastChunk)

	// Load dictionary from CSV
	if err := dh.manager.LoadFromCSV(chunkMsg.ClientID, chunkMsg.ChunkData, dh.parseFunc); err != nil {
		fmt.Printf("%s: Failed to load dictionary from CSV: %v\n", dh.workerName, err)
		return middleware.MessageMiddlewareMessageError
	}

	// Save dictionary chunk to file
	filePath := filepath.Join(dh.dictDir, chunkMsg.ClientID+".csv")
	if err := dh.manager.SaveDictionaryChunkToFile(chunkMsg.ClientID, filePath, chunkMsg.ChunkData); err != nil {
		fmt.Printf("%s: Warning - failed to save dictionary to file: %v\n", dh.workerName, err)
		// Continue processing even if save fails
	}

	// Check if this is the last chunk for the dictionary
	if chunkMsg.IsLastChunk {
		dh.manager.SetReady(chunkMsg.ClientID)
		fmt.Printf("%s: Received last chunk for FileID: %s - Dictionary is now ready for client %s\n",
			dh.workerName, chunkMsg.FileID, chunkMsg.ClientID)
	}

	return 0
}
