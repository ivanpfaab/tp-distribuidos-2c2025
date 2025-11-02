package parser

import (
	"encoding/csv"
	"fmt"
	"strings"
)

// BaseParser provides common CSV parsing utilities
type BaseParser struct{}

// ParseCSV parses CSV data and returns records, skipping header row
func (bp *BaseParser) ParseCSV(csvData string) ([][]string, error) {
	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}
	return records, nil
}

// IsHeaderRow checks if a record is a header row by checking if first field contains common header keywords
func (bp *BaseParser) IsHeaderRow(record []string, headerKeywords ...string) bool {
	if len(record) == 0 {
		return false
	}

	firstField := strings.ToLower(record[0])
	for _, keyword := range headerKeywords {
		if strings.Contains(firstField, strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

// SkipHeader skips header rows in records
func (bp *BaseParser) SkipHeader(records [][]string, headerKeywords ...string) [][]string {
	var dataRecords [][]string
	for _, record := range records {
		if !bp.IsHeaderRow(record, headerKeywords...) {
			dataRecords = append(dataRecords, record)
		}
	}
	return dataRecords
}

// ValidateRecordLength validates that a record has at least the required number of fields
func (bp *BaseParser) ValidateRecordLength(record []string, minLength int) bool {
	return len(record) >= minLength
}
