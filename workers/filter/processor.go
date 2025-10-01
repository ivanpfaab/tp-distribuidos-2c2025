package filter

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func filterQueryType1(step int, line string) bool {
	fields := strings.Split(line, ",")
	if len(fields) < 9 {
		fmt.Printf("Filter Worker: Malformed record (expected at least 9 fields): %s\n", line)
		return false
	}

	ts := strings.TrimSpace(fields[8])
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to parse timestamp '%s': %v\n", ts, err)
		return false
	}
	amm, err := strconv.Atoi(strings.TrimSpace(fields[8]))
	if err != nil {
		fmt.Printf("Filter Worker: Failed to parse integer from field[8] '%s': %v\n", fields[8], err)
		return false
	}
	pass := false
	switch step {
	case 1:
		// Keep only records from 2024 or 2025
		y := t.Year()
		pass = (y == 2024 || y == 2025)

	case 2:
		// Keep records between 06:00 and 23:00 inclusive
		hr := t.Hour()
		pass = (hr >= 6 && hr <= 23)

	case 3:
		// Keep records with ammount >= 75
		pass = amm >= 75
	default:

		pass = false
	}
	return pass
}

// processMessage processes a single message
func (fw *FilterWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	fmt.Printf("Filter Worker: Received message: %s\n", string(delivery.Body))

	// Deserialize the chunk message
	chunkMsg, err := chunk.DeserializeChunk(delivery.Body)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to deserialize chunk message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Process the chunk (filter logic would go here)
	fmt.Printf("Filter Worker: Processing chunk - QueryType: %d, Step: %d, ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.QueryType, chunkMsg.Step, chunkMsg.ClientID, chunkMsg.ChunkNumber)

	var responseBuilder strings.Builder
	responseSize := 0
	switch chunkMsg.QueryType {
	case chunk.QueryType1: // Filter
		fmt.Printf("Filter Worker: Applying filter for ClientID: %s, ChunkNumber: %d\n",
			chunkMsg.ClientID, chunkMsg.ChunkNumber)

		for _, line := range chunkMsg.ChunkData {

			lineStr := string(line)
			pass := filterQueryType1(chunkMsg.Step, lineStr)
			if pass {
				responseBuilder.WriteString(lineStr)
				responseBuilder.WriteByte('\n')
				responseSize += 1
			}
		}
	}
	// Send reply back to orchestrator
	chunkMsg.ChunkData = responseBuilder.String()
	chunkMsg.ChunkSize = responseSize
	return fw.sendReply(chunkMsg)
}

// sendReply sends a processed chunk as a reply back to the orchestrator
func (fw *FilterWorker) sendReply(chunkMsg *chunk.Chunk) middleware.MessageMiddlewareError {
	// Create a chunk message for serialization
	chunkMessage := chunk.NewChunkMessage(chunkMsg)

	// Serialize the chunk message for reply
	replyData, err := chunk.SerializeChunkMessage(chunkMessage)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to serialize reply message: %v\n", err)
		return middleware.MessageMiddlewareMessageError
	}

	// Send the reply to the orchestrator reply queue
	if err := fw.replyProducer.Send(replyData); err != 0 {
		fmt.Printf("Filter Worker: Failed to send reply to orchestrator: %v\n", err)
		return err
	}

	fmt.Printf("Filter Worker: Reply sent successfully for ClientID: %s, ChunkNumber: %d\n",
		chunkMsg.ClientID, chunkMsg.ChunkNumber)
	return 0
}
