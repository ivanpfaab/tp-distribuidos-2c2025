package main

import (
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func filterYear(line string) bool {
	fields := strings.Split(line, ",")
	if len(fields) < 9 {
		fmt.Printf("Year Filter Worker: Malformed record (expected at least 9 fields): %s\n", line)
		return false
	}

	ts := strings.TrimSpace(fields[8]) // created_at field
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Year Filter Worker: Failed to parse timestamp '%s': %v\n", ts, err)
		return false
	}
	y := t.Year()
	return (y == 2024 || y == 2025)
}

func YearFilterLogic(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError) {
	var responseBuilder strings.Builder
	dataRowCount := 0
	hasHeader := false

	lines := strings.Split(chunkMsg.ChunkData, "\n")

	// Include header row if it exists
	if len(lines) > 0 && lines[0] != "" {
		responseBuilder.WriteString(lines[0])
		responseBuilder.WriteByte('\n')
		hasHeader = true
	}

	// Process data rows - filter by year only
	for i, l := range lines {
		if i == 0 || l == "" {
			continue
		}
		pass := filterYear(l)
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

	fmt.Printf("Year Filter Worker: Filtered %d data records (total chunk size: %d)\n", dataRowCount, chunkMsg.ChunkSize)

	return *chunkMsg, 0
}

// processMessage processes a single message
func (yfw *YearFilterWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Year Filter Worker: Received message: %s\n", string(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Year Filter Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk (year filter logic)
	fmt.Printf("Year Filter Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	responseChunk, msgErr := YearFilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("Year Filter Worker: Failed to apply year filter logic: %v\n", msgErr)
		return msgErr
	}

	// Route the message based on query type
	return yfw.routeMessage(&responseChunk)
}

// routeMessage routes the processed chunk to the appropriate next stage
func (yfw *YearFilterWorker) routeMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Year Filter Worker: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Route based on query type
	switch chunkMsg.QueryType {
	case chunk.QueryType1, chunk.QueryType3:
		// Send to time filter for further processing
		if err := yfw.timeFilterProducer.Send(replyData); err != 0 {
			fmt.Printf("Year Filter Worker: Failed to send to time filter: %v\n", err)
			return err
		}
		fmt.Printf("Year Filter Worker: Sent to time filter for ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
	case chunk.QueryType2, chunk.QueryType4:
		// Send directly to reply bus
		if err := yfw.replyProducer.Send(replyData); err != 0 {
			fmt.Printf("Year Filter Worker: Failed to send to reply bus: %v\n", err)
			return err
		}
		fmt.Printf("Year Filter Worker: Sent to reply bus for ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)
	default:
		fmt.Printf("Year Filter Worker: Unknown QueryType: %d\n", chunkMsg.QueryType)
		return middleware.MessageMiddlewareMessageError
	}

	return 0
}
