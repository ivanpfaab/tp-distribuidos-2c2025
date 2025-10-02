package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

func filterYearAndHour(line string, ts_pos int) bool {
	fields := strings.Split(line, ",")
	if len(fields) < ts_pos+1 {
		fmt.Printf("Filter Worker: Malformed record (expected at least %d fields): %s\n", ts_pos+1, line)
		return false
	}

	ts := strings.TrimSpace(fields[ts_pos])
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to parse timestamp '%s': %v\n", ts, err)
		return false
	}
	y := t.Year()
	hr := t.Hour()
	fmt.Printf("Filter Worker: Year: %d, Hour: %d\n", y, hr)
	fmt.Printf("Filter Worker: Condition: %t\n", (y == 2024 || y == 2025) && (hr >= 6 && hr <= 23))
	return (y == 2024 || y == 2025) && (hr >= 6 && hr <= 23)
}

func filterYear(line string, ts_pos int) bool {
	fields := strings.Split(line, ",")
	if len(fields) < ts_pos+1 {
		fmt.Printf("Filter Worker: Malformed record (expected at least %d fields): %s\n", ts_pos+1, line)
		return false
	}

	ts := strings.TrimSpace(fields[ts_pos])
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to parse timestamp '%s': %v\n", ts, err)
		return false
	}
	y := t.Year()
	return (y == 2024 || y == 2025)
}

func filterAmmount(line string, amm_pos int) bool {
	fields := strings.Split(line, ",")
	if len(fields) < amm_pos+1 {
		fmt.Printf("Filter Worker: Malformed record (expected at least %d fields): %s\n", amm_pos+1, line)
		return false
	}

	amm, err := strconv.ParseFloat(strings.TrimSpace(fields[amm_pos]), 64)

	fmt.Printf("Filter Worker: Ammount: %f\n", amm)
	if err != nil {
		fmt.Printf("Filter Worker: Failed to parse float from field[%d] '%s': %v\n", amm_pos, fields[amm_pos], err)
		return false
	}
	fmt.Printf("Filter Worker: Condition: %t\n", amm >= 75)
	return amm >= 75
}

func FilterLogic(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError) {
	var responseBuilder strings.Builder
	responseSize := 0
	switch chunkMsg.QueryType {
	case chunk.QueryType1:
		lines := strings.Split(chunkMsg.ChunkData, "\n")
		// Skip header row (first line) and process data rows
		for i, l := range lines {
			if i == 0 || l == "" {
				continue
			}
			pass := filterYearAndHour(l, 8) && filterAmmount(l, 7)
			if pass {
				responseBuilder.WriteString(l)
				responseBuilder.WriteByte('\n')
				responseSize += 1
			}
		}
	case chunk.QueryType2:
		lines := strings.Split(chunkMsg.ChunkData, "\n")
		// Skip header row (first line) and process data rows
		for i, l := range lines {
			if i == 0 || l == "" {
				continue
			}
			pass := filterYear(l, 5)
			if pass {
				responseBuilder.WriteString(l)
				responseBuilder.WriteByte('\n')
				responseSize += 1
			}
		}
	case chunk.QueryType3:
		lines := strings.Split(chunkMsg.ChunkData, "\n")
		// Skip header row (first line) and process data rows
		for i, l := range lines {
			if i == 0 || l == "" {
				continue
			}
			pass := filterYearAndHour(l, 8)
			if pass {
				responseBuilder.WriteString(l)
				responseBuilder.WriteByte('\n')
				responseSize += 1
			}
		}
	case chunk.QueryType4:
		lines := strings.Split(chunkMsg.ChunkData, "\n")
		// Skip header row (first line) and process data rows
		for i, l := range lines {
			if i == 0 || l == "" {
				continue
			}
			pass := filterYear(l, 8)
			if pass {
				responseBuilder.WriteString(l)
				responseBuilder.WriteByte('\n')
				responseSize += 1
			}
		}

	default:
		fmt.Printf("Filter Worker: Unknown QueryType: %d\n", chunkMsg.QueryType)
		return chunk.Chunk{}, middleware.MessageMiddlewareMessageError
	}

	chunkMsg.ChunkData = responseBuilder.String()
	chunkMsg.ChunkSize = responseSize

	fmt.Printf("Filter Worker: Response Size: %d\n", responseSize)
	fmt.Printf("Filter Worker: Response: %s\n", responseBuilder.String())

	return *chunkMsg, 0

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

	responseChunk, msgErr := FilterLogic(chunkMsg)
	if msgErr != 0 {
		fmt.Printf("Filter Worker: Failed to apply filter logic: %v\n", msgErr)
		return msgErr
	}

	return fw.sendReply(&responseChunk)
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
