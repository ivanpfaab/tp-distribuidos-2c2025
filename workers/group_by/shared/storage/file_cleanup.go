package storage

import (
	"fmt"
	"os"
	"path/filepath"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

// FileCleanup handles file deletion operations
type FileCleanup struct{}

// NewFileCleanup creates a new file cleanup utility
func NewFileCleanup() *FileCleanup {
	return &FileCleanup{}
}

// DeleteFile deletes a single file
func (fc *FileCleanup) DeleteFile(filePath string) error {
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete file %s: %v", filePath, err)
	}
	testing_utils.LogInfo("FileCleanup", "Deleted file: %s", filePath)
	return nil
}

// DeleteFiles deletes multiple files
func (fc *FileCleanup) DeleteFiles(filePaths []string) error {
	for _, filePath := range filePaths {
		if err := fc.DeleteFile(filePath); err != nil {
			testing_utils.LogWarn("FileCleanup", "Failed to delete file %s: %v", filePath, err)
			// Continue deleting other files
		}
	}
	return nil
}

// DeleteClientFiles deletes all partition files for a client
func (fc *FileCleanup) DeleteClientFiles(fileManager *FileManager, clientID string) error {
	files, err := fileManager.GetClientFiles(clientID)
	if err != nil {
		return fmt.Errorf("failed to get files for deletion: %v", err)
	}

	if err := fc.DeleteFiles(files); err != nil {
		return err
	}

	testing_utils.LogInfo("FileCleanup", "Deleted %d partition files for client %s (Query %d)",
		len(files), clientID, fileManager.GetQueryType())
	return nil
}

// DeleteDirectory deletes an entire directory
func (fc *FileCleanup) DeleteDirectory(dirPath string) error {
	if err := os.RemoveAll(dirPath); err != nil {
		return fmt.Errorf("failed to delete directory %s: %v", dirPath, err)
	}
	testing_utils.LogInfo("FileCleanup", "Deleted directory: %s", dirPath)
	return nil
}

// CleanupEmptyDirectories removes empty directories in the given path
func (fc *FileCleanup) CleanupEmptyDirectories(basePath string) error {
	return filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue on error
		}

		if !info.IsDir() {
			return nil
		}

		// Try to remove directory (will fail if not empty)
		if err := os.Remove(path); err == nil {
			testing_utils.LogInfo("FileCleanup", "Removed empty directory: %s", path)
		}

		return nil
	})
}

