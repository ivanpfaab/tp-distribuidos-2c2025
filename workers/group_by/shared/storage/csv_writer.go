package storage

import (
	"encoding/csv"
	"fmt"
	"os"
)

// CSVWriter handles CSV writing operations
type CSVWriter struct{}

// NewCSVWriter creates a new CSV writer
func NewCSVWriter() *CSVWriter {
	return &CSVWriter{}
}

// AppendRecords appends records to a CSV file, writing header if file doesn't exist
func (w *CSVWriter) AppendRecords(filePath string, header []string, records [][]string) error {
	if len(records) == 0 {
		return nil
	}

	// Check if file exists
	fileExists := true
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fileExists = false
	}

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if this is a new file
	if !fileExists {
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Write all records
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
	}

	return nil
}

// AppendRecord appends a single record to a CSV file
func (w *CSVWriter) AppendRecord(filePath string, header []string, record []string) error {
	return w.AppendRecords(filePath, header, [][]string{record})
}

