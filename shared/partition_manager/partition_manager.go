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

	// Read last 2KB to check if file ends with newline
	readSize := int64(2048)
	if stat.Size() < readSize {
		readSize = stat.Size()
	}

	buffer := make([]byte, readSize)
	_, err = file.ReadAt(buffer, stat.Size()-readSize)
	if err != nil {
		return false, err
	}

	// Check if file ends with newline
	if buffer[len(buffer)-1] == '\n' {
		return false, nil // File ends with newline, no incomplete line
	}

	// If no newline found in last 2KB, read entire file
	if bytes.LastIndexByte(buffer, '\n') == -1 {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return false, err
		}
		if len(data) > 0 && data[len(data)-1] == '\n' {
			return false, nil
		}
	}

	return true, nil // Last line is incomplete
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

	// Read last 2KB to find last newline
	readSize := int64(2048)
	if stat.Size() < readSize {
		readSize = stat.Size()
	}

	buffer := make([]byte, readSize)
	_, err = file.ReadAt(buffer, stat.Size()-readSize)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// Find last newline
	lastNewline := bytes.LastIndexByte(buffer, '\n')
	if lastNewline == -1 {
		// No newline found, entire file is incomplete - truncate to 0
		return file.Truncate(0)
	}

	// Truncate to remove incomplete line
	newSize := stat.Size() - int64(len(buffer)-lastNewline-1)
	return file.Truncate(newSize)
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

	// Read last 2KB (should be enough for 2 lines)
	readSize := int64(2048)
	if stat.Size() < readSize {
		readSize = stat.Size()
	}

	buffer := make([]byte, readSize)
	_, err = file.ReadAt(buffer, stat.Size()-readSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Find all newlines in buffer (from end to start)
	lines := []string{}
	start := len(buffer)
	for i := len(buffer) - 1; i >= 0; i-- {
		if buffer[i] == '\n' {
			if start < len(buffer) {
				line := string(buffer[i+1 : start])
				lines = append([]string{line}, lines...)
				if len(lines) >= n {
					break
				}
			}
			start = i
		}
	}

	// Handle case where we didn't find enough newlines (file might be shorter)
	if start > 0 && len(lines) < n {
		line := string(buffer[0:start])
		lines = append([]string{line}, lines...)
	}

	// Return last N lines
	if len(lines) > n {
		return lines[len(lines)-n:], nil
	}
	return lines, nil
}

// WriteOnlyMissingLines writes only lines that haven't been written yet, avoiding duplicates
func (pm *PartitionManager) WriteOnlyMissingLines(filePath string, lastLines []string, newLines []string, opts WriteOptions) error {
	// If no last lines, write all new lines
	if len(lastLines) == 0 {
		return pm.appendToPartitionFile(filePath, newLines, opts)
	}

	testing_utils.LogInfo("Partition Manager", "new lines: %v", newLines)
	testing_utils.LogInfo("Partition Manager", "old lines: %v", lastLines)

	// Compare last written line with first new line
	lastWrittenLine := strings.TrimSuffix(lastLines[len(lastLines)-1], "\n")
	if len(newLines) > 0 {
		for i, newLine := range newLines {
			line := strings.TrimSuffix(newLine, "\n")
			if lastWrittenLine == line {
				// Last written line matches first new line - skip it
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
