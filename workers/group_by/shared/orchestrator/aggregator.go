package main

import (
	"fmt"
	"log"
	"sort"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/aggregation"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/storage"
)

// FileAggregator handles aggregation of client partition files
type FileAggregator struct {
	queryType   int
	fileManager *storage.FileManager
	fileCleanup *storage.FileCleanup
}

// NewFileAggregator creates a new file aggregator
func NewFileAggregator(queryType int, workerID int) *FileAggregator {
	return &FileAggregator{
		queryType:   queryType,
		fileManager: storage.NewFileManager(queryType, workerID),
		fileCleanup: storage.NewFileCleanup(),
	}
}

// FileResult represents a processed file result
type FileResult struct {
	PartitionNumber int
	CSVData         string
}

// AggregateClientFiles aggregates partition files for a client
// For Query 4: processes files individually (one result per file)
// For Query 2/3: aggregates all files into one result (backward compatible)
func (fa *FileAggregator) AggregateClientFiles(clientID string) ([]FileResult, error) {
	// Get all partition files for this client
	files, err := fa.fileManager.GetClientFiles(clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client files: %v", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files found for client %s", clientID)
	}

	// Sort files for consistent processing
	sort.Strings(files)

	// For Query 4: process files individually to reduce memory usage
	if fa.queryType == 4 {
		return fa.processFilesIndividually(files)
	}

	// For Query 2/3: aggregate all files (backward compatible)
	return fa.processFilesAggregated(files)
}

// processFilesIndividually processes each file separately (for Query 4)
func (fa *FileAggregator) processFilesIndividually(files []string) ([]FileResult, error) {
	results := make([]FileResult, 0, len(files))

	for _, filePath := range files {
		// Extract partition number from filename
		partitionNumber, err := common.ExtractPartitionFromFilename(filePath)
		if err != nil {
			log.Printf("Failed to extract partition from filename %s: %v", filePath, err)
			continue
		}

		// Create aggregator for this file
		aggregator, err := aggregation.NewAggregatorFromFile(fa.queryType, filePath)
		if err != nil {
			log.Printf("Failed to create aggregator for file %s: %v", filePath, err)
			continue
		}

		// Process single file - map goes out of scope immediately after
		csvData := fa.processSingleFile(aggregator, filePath)

		results = append(results, FileResult{
			PartitionNumber: partitionNumber,
			CSVData:         csvData,
		})
	}

	return results, nil
}

// processFilesAggregated aggregates all files together (for Query 2/3)
func (fa *FileAggregator) processFilesAggregated(files []string) ([]FileResult, error) {
	// Create aggregator from first file
	aggregator, err := aggregation.NewAggregatorFromFile(fa.queryType, files[0])
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregator: %v", err)
	}

	// Aggregate and format all files
	csvData := fa.aggregateAndFormat(aggregator, files)

	// Return as single result (partition number doesn't matter for Q2/Q3)
	return []FileResult{
		{
			PartitionNumber: 0,
			CSVData:         csvData,
		},
	}, nil
}

// processSingleFile processes a single file and returns formatted CSV
// The map goes out of scope immediately, making it eligible for GC
func (fa *FileAggregator) processSingleFile(aggregator aggregation.Aggregator, filePath string) string {
	// Initialize aggregated data map - will go out of scope when function returns
	aggregatedData := aggregator.InitializeDataMap()

	// Aggregate this single file
	if err := aggregator.AggregatePartitionFile(filePath, aggregatedData); err != nil {
		log.Printf("Failed to aggregate partition file %s: %v", filePath, err)
		// Return empty result if aggregation fails
		return aggregator.GetGrouper().GetHeader()
	}

	// Format output using the grouper
	grouper := aggregator.GetGrouper()
	result := grouper.FormatOutput(aggregatedData)

	// aggregatedData automatically goes out of scope here - eligible for GC
	return result
}

// aggregateAndFormat performs aggregation and formatting in a separate scope
// This ensures the aggregatedData map goes out of scope immediately after use,
// making it eligible for garbage collection without explicit cleanup
// Used for Query 2/3 where we aggregate all files together
func (fa *FileAggregator) aggregateAndFormat(aggregator aggregation.Aggregator, files []string) string {
	// Initialize aggregated data map - this will go out of scope when function returns
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

	// aggregatedData automatically goes out of scope here - no explicit cleanup needed
	return result
}

// CleanupClientFiles deletes all partition files for a client
func (fa *FileAggregator) CleanupClientFiles(clientID string) error {
	return fa.fileCleanup.DeleteClientFiles(fa.fileManager, clientID)
}
