package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/aggregation"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/storage"
)

// FileAggregator handles aggregation of client partition files
type FileAggregator struct {
	queryType    int
	fileManager  *storage.FileManager
	fileCleanup  *storage.FileCleanup
}

// NewFileAggregator creates a new file aggregator
func NewFileAggregator(queryType int, workerID int) *FileAggregator {
	return &FileAggregator{
		queryType:   queryType,
		fileManager: storage.NewFileManager(queryType, workerID),
		fileCleanup: storage.NewFileCleanup(),
	}
}

// AggregateClientFiles aggregates all partition files for a client into a single CSV
func (fa *FileAggregator) AggregateClientFiles(clientID string) (string, error) {
	// Get all partition files for this client
	files, err := fa.fileManager.GetClientFiles(clientID)
	if err != nil {
		return "", fmt.Errorf("failed to get client files: %v", err)
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no files found for client %s", clientID)
	}

	// Sort files for consistent processing
	sort.Strings(files)

	// Create aggregator from first file (to extract partition)
	aggregator, err := aggregation.NewAggregatorFromFile(fa.queryType, files[0])
	if err != nil {
		return "", fmt.Errorf("failed to create aggregator: %v", err)
	}

	// Initialize aggregated data map
	aggregatedData := aggregator.InitializeDataMap()

	// Aggregate all partition files
	for _, filePath := range files {
		if err := aggregator.AggregatePartitionFile(filePath, aggregatedData); err != nil {
			log.Printf("Failed to aggregate partition file %s: %v", filePath, err)
			// Continue with other files
			continue
		}
	}

	// Format output using the grouper
	grouper := aggregator.GetGrouper()
	result := grouper.FormatOutput(aggregatedData)

	// Clear the map to free memory immediately after formatting
	for k := range aggregatedData {
		delete(aggregatedData, k)
	}
	aggregatedData = nil

	return result, nil
}

// CleanupClientFiles deletes all partition files for a client
func (fa *FileAggregator) CleanupClientFiles(clientID string) error {
	return fa.fileCleanup.DeleteClientFiles(fa.fileManager, clientID)
}

