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
func (pm *PartitionManager) WritePartition(data PartitionData, chunkID string, opts WriteOptions) error {
	filePath := pm.GetPartitionFilePath(opts, data.Number)

	// Check if file exists and has incomplete last line
	hasIncomplete, incompleteLine, err := pm.checkIncompleteLastLine(filePath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to check incomplete line: %w", err)
	}

	if hasIncomplete {
		// Fix incomplete line and append new data
		return pm.fixIncompleteLineAndAppend(filePath, incompleteLine, data.Lines, opts)
	} else {
		// Normal append (file doesn't exist or is complete)
		return pm.appendToPartitionFile(filePath, data.Lines, opts)
	}
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

		testing_utils.LogInfo("Partition Manager", "Writing record: %v", record)

		// Write record
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	// Sync to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	testing_utils.LogInfo("Partition Manager", "Sleeping for 5 second")
	time.Sleep(5000 * time.Millisecond)

	return nil
}

// checkIncompleteLastLine checks if the last line in a file is incomplete (missing \n)
// Returns: (hasIncomplete, incompleteLine, error)
func (pm *PartitionManager) checkIncompleteLastLine(filePath string) (bool, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, "", nil // File doesn't exist, no incomplete line
		}
		return false, "", err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return false, "", err
	}

	if stat.Size() == 0 {
		return false, "", nil // Empty file
	}

	// Read last portion of file to find last line and check if it's complete.
	// We use 2KB as a buffer size because:
	// - CSV lines are typically short (50-200 bytes), so 2KB covers many lines
	// - This allows us to find the last newline without reading the entire file
	// - For very large files, this is more efficient than reading everything
	// - If no newline is found in 2KB, we fall back to reading the entire file
	// Note: 2KB is a heuristic; for typical CSV partition files this is sufficient
	readSize := int64(2048)
	if stat.Size() < readSize {
		readSize = stat.Size()
	}

	buffer := make([]byte, readSize)
	_, err = file.ReadAt(buffer, stat.Size()-readSize)
	if err != nil {
		return false, "", err
	}

	// Find last newline
	lastNewline := bytes.LastIndexByte(buffer, '\n')
	if lastNewline == -1 {
		// No newline found in last 2KB, entire file might be one incomplete line
		return pm.readIncompleteLine(filePath)
	}

	// Check if file ends with newline
	if buffer[len(buffer)-1] == '\n' {
		return false, "", nil // File ends with newline, no incomplete line
	}

	// Last line is incomplete, extract it
	incompleteLine := string(buffer[lastNewline+1:])
	return true, incompleteLine, nil
}

// readIncompleteLine reads the entire file if it's one incomplete line
func (pm *PartitionManager) readIncompleteLine(filePath string) (bool, string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return false, "", err
	}

	// Check if file ends with newline
	if len(data) > 0 && data[len(data)-1] == '\n' {
		return false, "", nil
	}

	// File is one incomplete line
	return true, string(data), nil
}

// fixIncompleteLineAndAppend fixes an incomplete line and appends new data
func (pm *PartitionManager) fixIncompleteLineAndAppend(
	filePath string,
	incompleteLine string,
	newLines []string,
	opts WriteOptions,
) error {
	// Open file for read-write
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file for read-write: %w", err)
	}
	defer file.Close()

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Always truncate to remove the incomplete line first
	// We'll decide later whether to write it back (if it doesn't match any new line)
	newSize := stat.Size() - int64(len(incompleteLine))
	if err := file.Truncate(newSize); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Check if incomplete line matches any of the new lines (to avoid duplicates)
	linesToWrite := newLines
	if len(newLines) > 0 {
		recordIncomplete, errIncomplete := parseCSVLine(incompleteLine)

		if errIncomplete == nil {
			// Check if incomplete line matches any of the new lines
			foundMatch := false
			for _, newLine := range newLines {
				recordNew, errNew := parseCSVLine(newLine)
				if errNew == nil {
					// Check if records are equal or if incomplete is a partial version of new line
					if recordsEqual(recordIncomplete, recordNew) || isPartialRecord(recordIncomplete, recordNew) {
						foundMatch = true
						break
					}
				}
			}

			if foundMatch {
				// Incomplete line matches a new line - it's already deleted, just write new lines
				linesToWrite = newLines
			} else {
				// No match found - incomplete line is different from all new lines
				// Write back the incomplete line (completed) along with new lines
				linesToWrite = append([]string{incompleteLine + "\n"}, newLines...)
			}
		} else {
			// Parsing failed, be safe and write back the incomplete line (completed)
			linesToWrite = append([]string{incompleteLine + "\n"}, newLines...)
		}
	} else {
		// No new lines, just write back the incomplete line (completed)
		linesToWrite = []string{incompleteLine + "\n"}
	}

	// Write all lines
	return pm.appendToPartitionFile(filePath, linesToWrite, opts)
}

// parseCSVLine parses a CSV line (with optional trailing newline) into a record
func parseCSVLine(line string) ([]string, error) {
	line = strings.TrimSuffix(line, "\n")
	reader := csv.NewReader(strings.NewReader(line))
	return reader.Read()
}

// recordsEqual compares two CSV records for equality
func recordsEqual(r1, r2 []string) bool {
	if len(r1) != len(r2) {
		return false
	}
	for i := range r1 {
		if r1[i] != r2[i] {
			return false
		}
	}
	return true
}

// isPartialRecord checks if incomplete (record1) is a partial version of complete (record2)
// This handles cases where a crash occurred mid-write, leaving a truncated record.
// Since CSV writes happen left-to-right, incomplete fields will be prefixes of complete fields.
// Examples:
//   - ["149", "E"] (incomplete) is a partial version of ["149", "Eve"] (complete)
//   - ["14", "E"] (incomplete) is a partial version of ["149", "Eve"] (complete)
//   - ["149", "Ev"] (incomplete) is a partial version of ["149", "Eve"] (complete)
func isPartialRecord(incomplete, complete []string) bool {
	// Both records must have the same number of fields
	if len(incomplete) != len(complete) {
		return false
	}

	// Check all fields: each incomplete field must be a prefix of the corresponding complete field
	// At least one field must be shorter (incomplete)
	hasIncompleteField := false
	for i := 0; i < len(incomplete); i++ {
		incompleteField := incomplete[i]
		completeField := complete[i]

		// Incomplete field must be shorter or equal, and must be a prefix of complete field
		if len(incompleteField) > len(completeField) {
			return false
		}
		if incompleteField != completeField[:len(incompleteField)] {
			return false
		}

		// Track if at least one field is actually incomplete (shorter)
		if len(incompleteField) < len(completeField) {
			hasIncompleteField = true
		}
	}

	// At least one field must be incomplete, otherwise records are identical
	return hasIncompleteField
}

// GetNumPartitions returns the number of partitions
func (pm *PartitionManager) GetNumPartitions() int {
	return pm.numPartitions
}

// GetPartitionsDir returns the partitions directory
func (pm *PartitionManager) GetPartitionsDir() string {
	return pm.partitionsDir
}
