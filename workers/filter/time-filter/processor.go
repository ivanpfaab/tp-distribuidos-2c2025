package main

import (
	"fmt"
	"strings"
	"time"

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
		if i == 0 && hasHeader {
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
