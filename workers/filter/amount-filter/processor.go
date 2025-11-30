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
