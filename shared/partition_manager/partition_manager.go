package partitionmanager

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// PartitionManager handles fault-tolerant partition writing
type PartitionManager struct {
	partitionsDir string
	numPartitions int
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager(partitionsDir string, numPartitions int) (*PartitionManager, error) {
	// Ensure partitions directory exists
	if err := os.MkdirAll(partitionsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create partitions directory: %w", err)
	}

	return &PartitionManager{
		partitionsDir: partitionsDir,
		numPartitions: numPartitions,
	}, nil
}

// GetPartitionFilePath returns the full path to a partition file
// Format: {clientID}-{filePrefix}-{partitionNum:03d}.csv
func (pm *PartitionManager) GetPartitionFilePath(opts WriteOptions, partitionNum int) string {
	filename := fmt.Sprintf("%s-%s-%03d.csv", opts.ClientID, opts.FilePrefix, partitionNum)
	return filepath.Join(pm.partitionsDir, filename)
}

// WritePartition writes partition data, handling incomplete writes
func (pm *PartitionManager) WritePartition(data PartitionData, opts WriteOptions) error {
	filePath := pm.GetPartitionFilePath(opts, data.Number)

	// Check if file exists and has incomplete last line
	hasIncomplete, err := pm.hasIncompleteLastLine(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to check incomplete line: %w", err)
	}

	if hasIncomplete {
		// Delete incomplete line by truncating file
		if err := pm.removeIncompleteLastLine(filePath); err != nil {
			return fmt.Errorf("failed to remove incomplete line: %w", err)
		}
	}

	// Normal append
	return pm.appendToPartitionFile(filePath, data.Lines, opts)
}

// appendToPartitionFile appends lines to a partition file
func (pm *PartitionManager) appendToPartitionFile(filePath string, lines []string, opts WriteOptions) error {
	// Check if file exists
	_, err := os.Stat(filePath)
	fileExists := !os.IsNotExist(err)

	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if this is a new file
	if !fileExists && len(opts.Header) > 0 {
		if err := writer.Write(opts.Header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write all lines (parse CSV lines back to records)
	for _, line := range lines {
		record, err := parseCSVLine(line)
		if err != nil {
			return fmt.Errorf("failed to parse CSV line: %w", err)
		}

		// Write record
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}

		testing_utils.LogInfo("Partition Manager", "Writing record: %v", record)
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Add delay in debug mode to make crashes more likely during testing
	if opts.DebugMode {
		testing_utils.LogInfo("Partition Manager", "Sleeping for 5 second")
		time.Sleep(5000 * time.Millisecond)
	}

	return nil
}

// hasIncompleteLastLine checks if the last line in a file is incomplete (missing \n)
func (pm *PartitionManager) hasIncompleteLastLine(filePath string) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil // File doesn't exist, no incomplete line
		}
		return false, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, err
	}

	if stat.Size() == 0 {
		return false, nil // Empty file
	}

	// Read the last byte to check if file ends with newline
	lastByte := make([]byte, 1)
	_, err = file.ReadAt(lastByte, stat.Size()-1)
	if err != nil {
		return false, err
	}

	// If last byte is not '\n', the last line is incomplete
	return lastByte[0] != '\n', nil
}

// removeIncompleteLastLine removes the incomplete last line by truncating the file
func (pm *PartitionManager) removeIncompleteLastLine(filePath string) error {
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
		return nil // Empty file, nothing to remove
	}

	// Read file backward in chunks until we find a newline
	const chunkSize = int64(512) // Read 512 bytes at a time
	offset := stat.Size()        // Start from the end

	for offset > 0 {
		// Calculate how much to read in this chunk
		readSize := chunkSize
		if offset < chunkSize {
			readSize = offset
		}

		// Read chunk from file (starting at offset - readSize)
		readOffset := offset - readSize
		buffer := make([]byte, readSize)
		_, err = file.ReadAt(buffer, readOffset)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}

		// Search backward in this chunk for newline
		lastNewline := bytes.LastIndexByte(buffer, '\n')
		if lastNewline != -1 {
			// Found newline at position lastNewline within this chunk
			// Truncate file at: readOffset + lastNewline + 1 (to keep the newline)
			newSize := readOffset + int64(lastNewline) + 1
			return file.Truncate(newSize)
		}

		// No newline in this chunk, continue reading backward
		offset = readOffset
	}

	// No newline found in entire file, truncate to 0
	return file.Truncate(0)
}

// parseCSVLine parses a CSV line (with optional trailing newline) into a record
func parseCSVLine(line string) ([]string, error) {
	line = strings.TrimSuffix(line, "\n")
	reader := csv.NewReader(strings.NewReader(line))
	return reader.Read()
}

// GetNumPartitions returns the number of partitions
func (pm *PartitionManager) GetNumPartitions() int {
	return pm.numPartitions
}

// GetPartitionsDir returns the partitions directory
func (pm *PartitionManager) GetPartitionsDir() string {
	return pm.partitionsDir
}

// GetLastLines reads the last N lines from a partition file
// Returns lines in the same format as partition.Lines (raw CSV strings like "48,Paul\n")
func (pm *PartitionManager) GetLastLines(filePath string, n int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // File doesn't exist, return empty
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		return []string{}, nil // Empty file
	}

	// Read file backward in chunks to find the Nth newline from the end
	const chunkSize = int64(512)
	offset := stat.Size()
	newlineCount := 0
	startOffset := stat.Size() // Position where we'll start reading from

	// Read backward until we find N newlines (or reach the beginning)
	for offset > 0 && newlineCount < n {
		// Calculate how much to read in this chunk
		readSize := chunkSize
		if offset < chunkSize {
			readSize = offset
		}

		// Read chunk from file (starting at offset - readSize)
		readOffset := offset - readSize
		buffer := make([]byte, readSize)
		_, err = file.ReadAt(buffer, readOffset)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		// Count newlines in this chunk (from end to start)
		for i := len(buffer) - 1; i >= 0; i-- {
			if buffer[i] == '\n' {
				newlineCount++
				if newlineCount == n {
					// Found the Nth newline - record position (after the newline)
					startOffset = readOffset + int64(i) + 1
					break
				}
			}
		}

		offset = readOffset
	}

	// If we didn't find N newlines, start from the beginning of the file
	if newlineCount < n {
		startOffset = 0
	}

	// Read from startOffset to the end of the file
	readSize := stat.Size() - startOffset
	data := make([]byte, readSize)
	_, err = file.ReadAt(data, startOffset)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Parse CSV data from the read portion
	reader := csv.NewReader(strings.NewReader(string(data)))
	allRecords, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	// Filter out header if present
	records := allRecords
	if len(records) > 0 && len(records[0]) > 0 {
		// Check if first record is header (common headers: user_id, id, etc.)
		firstField := strings.ToLower(records[0][0])
		if firstField == "user_id" || firstField == "id" {
			records = records[1:]
		}
	}

	// Convert last N records to the same format as partition.Lines
	// Format: "field1,field2\n" (matching partitionData function)
	result := []string{}
	start := len(records) - n
	if start < 0 {
		start = 0
	}

	for i := start; i < len(records); i++ {
		// Convert record back to CSV line format (same as partitionData does)
		csvLine := strings.Join(records[i], ",") + "\n"
		result = append(result, csvLine)
	}

	return result, nil
}

// WriteOnlyMissingLines writes only lines that haven't been written yet, avoiding duplicates
func (pm *PartitionManager) WriteOnlyMissingLines(filePath string, lastLines []string, newLines []string, opts WriteOptions) error {
	// If no last lines, write all new lines
	if len(lastLines) == 0 {
		return pm.appendToPartitionFile(filePath, newLines, opts)
	}

	// Compare last written line with first new line
	lastWrittenLine := strings.TrimSuffix(lastLines[len(lastLines)-1], "\n")
	if len(newLines) > 0 {
		for i, newLine := range newLines {
			line := strings.TrimSuffix(newLine, "\n")
			if lastWrittenLine == line {
				// Last written line matches first new line - skip it
				if i+1 >= len(newLines) {
					return nil
				}
				return pm.appendToPartitionFile(filePath, newLines[(i+1):], opts)
			}
		}
	}

	return pm.appendToPartitionFile(filePath, newLines, opts)
}

// RecoverIncompleteWrites checks all partition files in the directory for incomplete writes and fixes them
func (pm *PartitionManager) RecoverIncompleteWrites() (int, error) {
	fixedCount := 0

	// Read all files in partitions directory
	entries, err := os.ReadDir(pm.partitionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // Directory doesn't exist, nothing to recover
		}
		return 0, fmt.Errorf("failed to read partitions directory: %w", err)
	}

	// Check each CSV file for incomplete writes
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		filePath := filepath.Join(pm.partitionsDir, entry.Name())
		hasIncomplete, err := pm.hasIncompleteLastLine(filePath)
		if err != nil {
			continue // Skip files that can't be read
		}

		if hasIncomplete {
			if err := pm.removeIncompleteLastLine(filePath); err != nil {
				continue // Skip files that can't be fixed
			}
			fixedCount++
		}
	}

	return fixedCount, nil
}
