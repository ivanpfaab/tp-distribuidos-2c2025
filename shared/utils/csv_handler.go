package utils

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CSVHandler provides utilities for CSV file operations
type CSVHandler struct {
	baseDir string
}

// NewCSVHandler creates a new CSV handler
func NewCSVHandler(baseDir string) *CSVHandler {
	return &CSVHandler{
		baseDir: baseDir,
	}
}

// AppendRow appends a row to a CSV file, writing the header if the file is new
func (h *CSVHandler) AppendRow(filePath string, row []string, columns []string) error {
	return h.AppendRows(filePath, [][]string{row}, columns)
}

// AppendRows appends multiple rows to a CSV file, writing the header if the file is new
// This method keeps the file open for all writes and only syncs once at the end (more efficient)
func (h *CSVHandler) AppendRows(filePath string, rows [][]string, columns []string) error {
	if len(rows) == 0 {
		return nil
	}

	// Ensure directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if file is new
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 && len(columns) > 0 {
		if err := writer.Write(columns); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write all data rows
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	// Sync once after all rows written
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// ReadFileStreaming reads CSV file line-by-line and calls processRow for each row (memory efficient)
// Skips the header row
func (h *CSVHandler) ReadFileStreaming(filePath string, processRow func([]string) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header (first line) - skip it
	_, err = reader.Read()
	if err == io.EOF {
		return nil // Empty file
	}
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Process data rows one at a time
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		if err := processRow(row); err != nil {
			return fmt.Errorf("error processing row: %w", err)
		}
	}

	return nil
}

// ReadFileStreamingWithHeader reads CSV file line-by-line, validates header, and calls processRow for each row
func (h *CSVHandler) ReadFileStreamingWithHeader(filePath string, expectedColumns []string, processRow func([]string) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read and validate header
	header, err := reader.Read()
	if err == io.EOF {
		return nil // Empty file
	}
	if err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	if len(header) < len(expectedColumns) {
		return fmt.Errorf("invalid CSV header: expected at least %d columns, got %d", len(expectedColumns), len(header))
	}

	// Process data rows one at a time
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %w", err)
		}

		if err := processRow(row); err != nil {
			return fmt.Errorf("error processing row: %w", err)
		}
	}

	return nil
}

// HasIncompleteLastRow checks if the last row in a CSV file is incomplete (missing \n)
func (h *CSVHandler) HasIncompleteLastRow(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size() == 0 {
		return false, nil
	}

	// Read the last byte to check if file ends with newline
	lastByte := make([]byte, 1)
	_, err = file.ReadAt(lastByte, stat.Size()-1)
	if err != nil {
		return false, err
	}

	return lastByte[0] != '\n', nil
}

// RemoveIncompleteLastRow removes the incomplete last row by truncating the file
func (h *CSVHandler) RemoveIncompleteLastRow(filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil
	}

	// Read file backward in chunks until we find a newline
	const chunkSize = int64(512)
	offset := stat.Size()

	for offset > 0 {
		readSize := chunkSize
		if offset < chunkSize {
			readSize = offset
		}

		readOffset := offset - readSize
		buffer := make([]byte, readSize)
		_, err = file.ReadAt(buffer, readOffset)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Search backward for newline
		lastNewline := -1
		for i := len(buffer) - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				lastNewline = i
				break
			}
		}

		if lastNewline != -1 {
			newSize := readOffset + int64(lastNewline) + 1
			return file.Truncate(newSize)
		}

		offset = readOffset
	}

	// No newline found, truncate to 0
	return file.Truncate(0)
}

// DeleteFile deletes a CSV file
func (h *CSVHandler) DeleteFile(filePath string) error {
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}
