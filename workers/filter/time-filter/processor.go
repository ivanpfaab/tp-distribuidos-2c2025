package main

import (
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func filterHour(line string) bool {
	fields := strings.Split(line, ",")
	if len(fields) < 9 {
		fmt.Printf("Time Filter Worker: Malformed record (expected at least 9 fields): %s\n", line)
		return false
	}

	ts := strings.TrimSpace(fields[8]) // created_at field
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Time Filter Worker: Failed to parse timestamp '%s': %v\n", ts, err)
		return false
	}
	hr := t.Hour()
	return (hr >= 6 && hr <= 23)
}

func TimeFilterLogic(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError) {
	var responseBuilder strings.Builder
	dataRowCount := 0
	hasHeader := false

	lines := strings.Split(chunkMsg.ChunkData, "\n")

	// Include header row if it exists
	if len(lines) > 0 && strings.Contains(lines[0], "created_at") {
		responseBuilder.WriteString(lines[0])
		responseBuilder.WriteByte('\n')
		hasHeader = true
	}

	// Process data rows - filter by hour only
	for i, l := range lines {
		if i == 0 || l == "" {
			continue
		}
		pass := filterHour(l)
		if pass {
			responseBuilder.WriteString(l)
			responseBuilder.WriteByte('\n')
			dataRowCount += 1
		}
	}

	chunkMsg.ChunkData = responseBuilder.String()
	// ChunkSize should include header if present
	if hasHeader {
		chunkMsg.ChunkSize = dataRowCount + 1
	} else {
		chunkMsg.ChunkSize = dataRowCount
	}

	fmt.Printf("Time Filter Worker: Filtered %d data records (total chunk size: %d)\n", dataRowCount, chunkMsg.ChunkSize)

	return *chunkMsg, 0
}

// processMessage processes a single message
func (tfw *TimeFilterWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Time Filter Worker: Received message: %s\n", string(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Time Filter Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk (time filter logic)
	fmt.Printf("Time Filter Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	responseChunk, msgErr := TimeFilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("Time Filter Worker: Failed to apply time filter logic: %v\n", msgErr)
		return msgErr
	}

	// Route the message based on query type
	return tfw.routeMessage(&responseChunk)
}

// routeMessage routes the processed chunk to the appropriate next stage
func (tfw *TimeFilterWorker) routeMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Time Filter Worker: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Route based on query type
	switch chunkMsg.QueryType {
	case chunk.QueryType1:
		// Send to amount filter for further processing
		if err := tfw.amountFilterProducer.Send(replyData); err != 0 {
			fmt.Printf("Time Filter Worker: Failed to send to amount filter: %v\n", err)
			return err
		}
		fmt.Printf("Time Filter Worker: Sent to amount filter for ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
	case chunk.QueryType3:
		// Send directly to reply bus
		if err := tfw.replyProducer.Send(replyData); err != 0 {
			fmt.Printf("Time Filter Worker: Failed to send to reply bus: %v\n", err)
			return err
		}
		fmt.Printf("Time Filter Worker: Sent to reply bus for ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
	default:
		fmt.Printf("Time Filter Worker: Unknown QueryType: %d\n", chunkMsg.QueryType)
		return middleware.MessageMiddlewareMessageError
	}

	return 0
}
