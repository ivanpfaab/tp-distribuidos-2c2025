package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

const (
	GroupByDataDir = "/app/groupby-data"
)

// FileManager handles file I/O operations for group by results
type FileManager struct {
	queryType int
	baseDir   string
}

// NewFileManager creates a new file manager for a specific query type
func NewFileManager(queryType int) *FileManager {
	baseDir := filepath.Join(GroupByDataDir, fmt.Sprintf("q%d", queryType))
	return &FileManager{
		queryType: queryType,
		baseDir:   baseDir,
	}
}

// GetFilePath returns the file path for a given client and partition
func (fm *FileManager) GetFilePath(clientID string, partition int) string {
	clientDir := filepath.Join(fm.baseDir, clientID)

	var filename string
	switch fm.queryType {
	case 2, 3:
		// For Query 2 and 3: partition maps to semester
		// Partition 0 = S1-2024, Partition 1 = S2-2024, Partition 2 = S1-2025
		year, semester := fm.partitionToSemester(partition)
		filename = fmt.Sprintf("%d-%d.json", year, semester)
	case 4:
		// For Query 4: partition is just the partition number
		filename = fmt.Sprintf("%d.json", partition)
	default:
		filename = fmt.Sprintf("%d.json", partition)
	}

	return filepath.Join(clientDir, filename)
}

// partitionToSemester converts partition number to year and semester
func (fm *FileManager) partitionToSemester(partition int) (int, int) {
	// Partition 0 = 2024-S1
	// Partition 1 = 2024-S2
	// Partition 2 = 2025-S1
	switch partition {
	case 0:
		return 2024, 1
	case 1:
		return 2024, 2
	case 2:
		return 2025, 1
	default:
		// Fallback for unexpected partitions
		return 2024, 1
	}
}

// EnsureClientDir ensures the client directory exists
func (fm *FileManager) EnsureClientDir(clientID string) error {
	clientDir := filepath.Join(fm.baseDir, clientID)
	if err := os.MkdirAll(clientDir, 0755); err != nil {
		return fmt.Errorf("failed to create client directory %s: %v", clientDir, err)
	}
	return nil
}

// LoadData loads aggregated data from a JSON file
// Returns nil if file doesn't exist (first chunk for this partition)
func (fm *FileManager) LoadData(clientID string, partition int) (map[string]interface{}, error) {
	filePath := fm.GetFilePath(clientID, partition)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		testing_utils.LogInfo("FileManager", "File does not exist, starting fresh: %s", filePath)
		return make(map[string]interface{}), nil
	}

	// Read file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", filePath, err)
	}

	// Parse JSON
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil { // TODO: check if it is permitted to use json and unmarshal
		return nil, fmt.Errorf("failed to unmarshal JSON from %s: %v", filePath, err)
	}

	// testing_utils.LogInfo("FileManager", "Loaded data from %s (%d top-level keys)", filePath, len(result))
	return result, nil
}

// SaveData saves aggregated data to a JSON file
func (fm *FileManager) SaveData(clientID string, partition int, data map[string]interface{}) error {
	filePath := fm.GetFilePath(clientID, partition)

	// Ensure client directory exists
	if err := fm.EnsureClientDir(clientID); err != nil {
		return err
	}

	// Marshal data to JSON
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %v", err)
	}

	// Write to file
	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %v", filePath, err)
	}

	return nil
}

// FileExists checks if a file exists for a given client and partition
func (fm *FileManager) FileExists(clientID string, partition int) bool {
	filePath := fm.GetFilePath(clientID, partition)
	_, err := os.Stat(filePath)
	return err == nil
}
