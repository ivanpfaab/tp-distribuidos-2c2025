package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// CSVReader provides utilities for reading CSV files
type CSVReader struct {
	bufferSize int
}

// NewCSVReader creates a new CSV reader with default buffer size
func NewCSVReader() *CSVReader {
	return &CSVReader{
		bufferSize: 64 * 1024, // 64KB default buffer
	}
}

// NewCSVReaderWithBuffer creates a new CSV reader with custom buffer size
func NewCSVReaderWithBuffer(bufferSize int) *CSVReader {
	return &CSVReader{
		bufferSize: bufferSize,
	}
}

// ReadFile reads a CSV file and returns all records (excluding header)
// Returns: (records, header, error)
func (r *CSVReader) ReadFile(filePath string) ([][]string, []string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	bufferedFile := bufio.NewReaderSize(file, r.bufferSize)
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true

	// Read header
	header, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return [][]string{}, header, nil // Empty file
		}
		return nil, nil, fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read records from %s: %v", filePath, err)
	}

	return records, header, nil
}

// ReadFileStreaming reads a CSV file and processes records one at a time
// Calls processRecord for each record (excluding header)
func (r *CSVReader) ReadFileStreaming(filePath string, processRecord func([]string) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	bufferedFile := bufio.NewReaderSize(file, r.bufferSize)
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true

	// Skip header
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return nil // Empty file
		}
		return fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Process records one at a time
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			return fmt.Errorf("error reading from %s: %v", filePath, err)
		}

		if err := processRecord(record); err != nil {
			return err
		}
	}

	return nil
}

// ParseCSVData parses CSV data string and returns records (excluding header if present)
func ParseCSVData(csvData string) ([][]string, error) {
	if csvData == "" {
		return [][]string{}, nil
	}

	reader := csv.NewReader(strings.NewReader(csvData))
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %v", err)
	}

	if len(records) == 0 {
		return [][]string{}, nil
	}

	// Check if first row looks like a header (optional - caller can decide)
	// For now, we'll return all records and let the caller handle header detection
	return records, nil
}

// ValidateHeader validates that a CSV header matches the expected schema
func ValidateHeader(header []string, expectedSchema []string) error {
	if len(header) != len(expectedSchema) {
		return fmt.Errorf("schema field count mismatch: expected %d, got %d", len(expectedSchema), len(header))
	}

	for i, expectedField := range expectedSchema {
		actualField := strings.TrimSpace(header[i])
		if actualField != expectedField {
			return fmt.Errorf("schema field mismatch at position %d: expected '%s', got '%s'", i, expectedField, actualField)
		}
	}

	return nil
}

