package storage

import (
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// CSVReader wraps the common CSV reader for storage operations
type CSVReader struct {
	reader *common.CSVReader
}

// NewCSVReader creates a new CSV reader for storage operations
func NewCSVReader() *CSVReader {
	return &CSVReader{
		reader: common.NewCSVReader(),
	}
}

// ReadFile reads a CSV file and returns all records (excluding header)
func (r *CSVReader) ReadFile(filePath string) ([][]string, []string, error) {
	return r.reader.ReadFile(filePath)
}

// ReadFileStreaming reads a CSV file and processes records one at a time
func (r *CSVReader) ReadFileStreaming(filePath string, processRecord func([]string) error) error {
	return r.reader.ReadFileStreaming(filePath, processRecord)
}

