package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
)

// SchemaInfo holds information about different file schemas
type SchemaInfo struct {
	TimestampFieldIndex int
	TimestampFieldName  string
	MinFieldsRequired   int
}

// getSchemaInfo returns schema information based on file ID
func getSchemaInfo(fileID string) SchemaInfo {
	switch {
	case strings.HasPrefix(fileID, "TR"): // Transaction files
		return SchemaInfo{
			TimestampFieldIndex: 8,
			TimestampFieldName:  "created_at",
			MinFieldsRequired:   9,
		}
	case strings.HasPrefix(fileID, "TI"): // Transaction Items files
		return SchemaInfo{
			TimestampFieldIndex: 5,
			TimestampFieldName:  "created_at",
			MinFieldsRequired:   5,
		}
	default:
		// Default to transaction schema for unknown file types
		return SchemaInfo{
			TimestampFieldIndex: 8,
			TimestampFieldName:  "created_at",
			MinFieldsRequired:   9,
		}
	}
}

func filterYear(line string, fileID string) bool {
	schema := getSchemaInfo(fileID)

	fields := strings.Split(line, ",")
	if len(fields) < schema.MinFieldsRequired {
		fmt.Printf("Year Filter Worker: Malformed record for file %s (expected at least %d fields): %s\n",
			fileID, schema.MinFieldsRequired, line)
		return false
	}

	ts := strings.TrimSpace(fields[schema.TimestampFieldIndex])
	t, err := time.ParseInLocation("2006-01-02 15:04:05", ts, time.Local)
	if err != nil {
		fmt.Printf("Year Filter Worker: Failed to parse timestamp '%s' for file %s: %v\n", ts, fileID, err)
		return false
	}
	y := t.Year()
	return (y == 2024 || y == 2025)
}

func YearFilterLogic(chunkMsg *chunk.Chunk) (chunk.Chunk, middleware.MessageMiddlewareError) {
	var responseBuilder strings.Builder
	dataRowCount := 0
	hasHeader := false
	schema := getSchemaInfo(chunkMsg.FileID)

	lines := strings.Split(chunkMsg.ChunkData, "\n")

	// Include header row if it exists and has the expected timestamp field
	if len(lines) > 0 && strings.Contains(lines[0], schema.TimestampFieldName) {
		responseBuilder.WriteString(lines[0])
		responseBuilder.WriteByte('\n')
		hasHeader = true
	}

	// Process data rows - filter by year only
	for i, l := range lines {
		// Skip header row if it exists
		if i == 0 && hasHeader {
			continue
		}
		// Skip empty or whitespace-only lines
		if strings.TrimSpace(l) == "" {
			continue
		}
		pass := filterYear(l, chunkMsg.FileID)
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

	fmt.Printf("Year Filter Worker: Filtered %d data records for file %s (total chunk size: %d)\n",
		dataRowCount, chunkMsg.FileID, chunkMsg.ChunkSize)

	return *chunkMsg, 0
}
