package main

import (
	"fmt"
	"strconv"
	"strings"

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
func (afw *AmountFilterWorker) processMessage(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {

	// Check if already processed
	if afw.messageManager.IsProcessed(chunkMsg.ID) {
		fmt.Printf("Amount Filter Worker: Chunk ID %s already processed, skipping\n", chunkMsg.ID)
		return 0 // Return 0 to ACK (handled by createCallback)
	}

	// Process the chunk (amount filter logic)
	fmt.Printf("Amount Filter Worker: Processing chunk - QueryType: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	responseChunk, msgErr := AmountFilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("Amount Filter Worker: Failed to apply amount filter logic: %v\n", msgErr)
		return msgErr
	}

	// Send to reply bus (pass chunk ID for marking as processed)
	return afw.sendReply(&responseChunk, chunkMsg.ID)
}

// sendReply sends a processed chunk as a reply back to the reply bus
func (afw *AmountFilterWorker) sendReply(chunkMsg *chunk.Chunk, chunkID string) middleware.MessageMiddlewareError {
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

	// Mark as processed (must be after successful send)
	if err := afw.messageManager.MarkProcessed(chunkID); err != nil {
		fmt.Printf("Amount Filter Worker: Failed to mark chunk as processed: %v\n", err)
		return middleware.MessageMiddlewareMessageError // Will NACK and requeue
	}

	fmt.Printf("Amount Filter Worker: Reply sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}
