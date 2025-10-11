package main

import (
	"fmt"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func filterAmount(line string) bool {
	fields := strings.Split(line, ",")
	if len(fields) < 8 {
		fmt.Printf("Amount Filter Worker: Malformed record (expected at least 8 fields): %s\n", line)
		return false
	}

	amount, err := strconv.ParseFloat(strings.TrimSpace(fields[7]), 64) // final_amount field
	if err != nil {
		fmt.Printf("Amount Filter Worker: Failed to parse amount from field '%s': %v\n", fields[7], err)
		return false
	}

	return amount >= 75
}

func AmountFilterLogic(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError) {
	var responseBuilder strings.Builder
	responseSize := 0
	hasHeader := false

	lines := strings.Split(chunkMsg.ChunkData, "\n")

	// Include header row
	if len(lines) > 0 && strings.Contains(lines[0], "final_amount") {
		responseBuilder.WriteString(lines[0])
		responseBuilder.WriteByte('\n')
		hasHeader = true
	}

	// Process data rows - filter by amount only
	for i, l := range lines {
		if i == 0 && hasHeader {
			continue
		}
		pass := filterAmount(l)
		if pass {
			responseBuilder.WriteString(l)
			responseBuilder.WriteByte('\n')
			responseSize += 1
		}
	}

	chunkMsg.ChunkData = responseBuilder.String()
	chunkMsg.ChunkSize = responseSize

	fmt.Printf("Amount Filter Worker: Filtered %d records\n", responseSize)

	return *chunkMsg, 0
}

// processMessage processes a single message
func (afw *AmountFilterWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Amount Filter Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk (amount filter logic)
	fmt.Printf("Amount Filter Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	responseChunk, msgErr := AmountFilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("Amount Filter Worker: Failed to apply amount filter logic: %v\n", msgErr)
		return msgErr
	}

	// Send to reply bus
	return afw.sendReply(&responseChunk)
}

// sendReply sends a processed chunk as a reply back to the reply bus
func (afw *AmountFilterWorker) sendReply(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message for reply
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Amount Filter Worker: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the reply to the reply bus
	if err := afw.replyProducer.Send(replyData); err != 0 {
		fmt.Printf("Amount Filter Worker: Failed to send reply to reply bus: %v\n", err)
		return err
	}

	fmt.Printf("Amount Filter Worker: Reply sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}
