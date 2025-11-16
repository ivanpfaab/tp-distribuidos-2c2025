package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// FileManager handles file path operations and directory management
type FileManager struct {
	queryType int
	workerID  int
	baseDir   string
}

// GetQueryType returns the query type
func (fm *FileManager) GetQueryType() int {
	return fm.queryType
}

// NewFileManager creates a new file manager for a specific query type and worker
func NewFileManager(queryType int, workerID int) *FileManager {
	baseDir := common.GetBaseDir(queryType, workerID)
	return &FileManager{
		queryType: queryType,
		workerID:  workerID,
		baseDir:   baseDir,
	}
}

// GetBaseDir returns the base directory for this file manager
func (fm *FileManager) GetBaseDir() string {
	return fm.baseDir
}

// GetPartitionFilePath returns the file path for a given client and partition
func (fm *FileManager) GetPartitionFilePath(clientID string, partition int) string {
	return common.GetPartitionFilePath(fm.baseDir, clientID, fm.queryType, partition)
}

// EnsureBaseDir ensures the base directory exists
func (fm *FileManager) EnsureBaseDir() error {
	if err := os.MkdirAll(fm.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create base directory %s: %v", fm.baseDir, err)
	}
	return nil
}

// FileExists checks if a file exists
func (fm *FileManager) FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

// GetClientFiles returns all partition files for a given client
func (fm *FileManager) GetClientFiles(clientID string) ([]string, error) {
	prefix := common.GetClientFilePattern(clientID, fm.queryType)
	var files []string

	entries, err := os.ReadDir(fm.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base directory %s: %v", fm.baseDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".csv" {
			if len(entry.Name()) >= len(prefix) && entry.Name()[:len(prefix)] == prefix {
				files = append(files, filepath.Join(fm.baseDir, entry.Name()))
			}
		}
	}

	return files, nil
}

