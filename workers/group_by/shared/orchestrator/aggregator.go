package main

import (
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/aggregation"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/storage"
)

// FileAggregator handles aggregation of client partition files
type FileAggregator struct {
	queryType     int
	workerID      int
	numPartitions int
	numWorkers    int
	fileManager   *storage.FileManager
	fileCleanup   *storage.FileCleanup
}

// NewFileAggregator creates a new file aggregator
func NewFileAggregator(queryType int, workerID int, numPartitions int, numWorkers int) *FileAggregator {
	return &FileAggregator{
		queryType:     queryType,
		workerID:      workerID,
		numPartitions: numPartitions,
		numWorkers:    numWorkers,
		fileManager:   storage.NewFileManager(queryType, workerID),
		fileCleanup:   storage.NewFileCleanup(),
	}
}

// FileResult represents a processed file result
type FileResult struct {
	PartitionNumber int
	CSVData         string
}

// AggregateClientFiles aggregates partition files for a client
// All queries: processes files individually (one chunk per partition file)
// This keeps memory usage low - only one partition's data in memory at a time
// Sends empty chunks for partitions with no data to ensure downstream expects correct count
func (fa *FileAggregator) AggregateClientFiles(clientID string) ([]FileResult, error) {
	// Get all partition files for this client
	files, err := fa.fileManager.GetClientFiles(clientID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client files: %v", err)
	}

	// Build a map of partition number to file path for quick lookup
	partitionFiles := make(map[int]string)
	for _, filePath := range files {
		partitionNumber, err := common.ExtractPartitionFromFilename(filePath)
		if err != nil {
			log.Printf("Failed to extract partition from filename %s: %v", filePath, err)
			continue
		}
		partitionFiles[partitionNumber] = filePath
	}

	// All queries: process all owned partitions (send empty chunks for missing ones)
	return fa.processAllOwnedPartitions(clientID, partitionFiles)
}

// processAllOwnedPartitions processes all partitions owned by this worker
// Sends data chunks for partitions with files, empty chunks for partitions without files
func (fa *FileAggregator) processAllOwnedPartitions(clientID string, partitionFiles map[int]string) ([]FileResult, error) {
	// Calculate which partitions this worker owns
	// MUST match the worker's logic exactly: partition % numWorkers == (workerID % numWorkers)
	// Worker 1: partitions 1, 6, 11, 16, 21, ... (partition % 5 == 1)
	// Worker 2: partitions 2, 7, 12, 17, 22, ... (partition % 5 == 2)
	// Worker 5: partitions 0, 5, 10, 15, 20, ... (partition % 5 == 0)
	ownedPartitions := make([]int, 0, fa.numPartitions/fa.numWorkers+1)
	targetRemainder := fa.workerID % fa.numWorkers
	for partition := 0; partition < fa.numPartitions; partition++ {
		if partition%fa.numWorkers == targetRemainder {
			ownedPartitions = append(ownedPartitions, partition)
		}
	}

	log.Printf("Worker %d owns %d partitions out of %d total", fa.workerID, len(ownedPartitions), fa.numPartitions)

	results := make([]FileResult, 0, len(ownedPartitions))

	for _, partitionNumber := range ownedPartitions {
		filePath, hasFile := partitionFiles[partitionNumber]

		if hasFile {
			// Process the file
			aggregator, err := aggregation.NewAggregatorFromFile(fa.queryType, filePath)
			if err != nil {
				log.Printf("Failed to create aggregator for file %s: %v", filePath, err)
				// Send empty chunk instead
				csvData := fa.getEmptyChunkData()
				results = append(results, FileResult{
					PartitionNumber: partitionNumber,
					CSVData:         csvData,
				})
				continue
			}

			csvData := fa.processSingleFile(aggregator, filePath)
			results = append(results, FileResult{
				PartitionNumber: partitionNumber,
				CSVData:         csvData,
			})
		} else {
			// No file for this partition - send empty chunk
			log.Printf("Client %s: Partition %d has no data, sending empty chunk", clientID, partitionNumber)
			csvData := fa.getEmptyChunkData()
			results = append(results, FileResult{
				PartitionNumber: partitionNumber,
				CSVData:         csvData,
			})
		}
	}

	return results, nil
}

// getEmptyChunkData returns an empty CSV with just the header for this query type
// Must match the aggregated output format (after grouping)
func (fa *FileAggregator) getEmptyChunkData() string {
	switch fa.queryType {
	case 2:
		// Query 2: After aggregation, includes year, month, item_id, quantity, subtotal, count
		return "year,month,item_id,quantity,subtotal,count\n"
	case 3:
		// Query 3: After aggregation, includes year, semester, store_id, total_final_amount, count
		return "year,semester,store_id,total_final_amount,count\n"
	case 4:
		// Query 4: After aggregation, includes year, semester, store_id, user_id, purchase_count
		return "year,semester,store_id,user_id,purchase_count\n"
	default:
		return "\n"
	}
}

// processFilesAggregated aggregates all files together
// NOTE: This approach holds all partition data in memory simultaneously
// Currently unused - kept for reference only
func (fa *FileAggregator) processFilesAggregated(files []string) ([]FileResult, error) {
	// Create aggregator from first file
	aggregator, err := aggregation.NewAggregatorFromFile(fa.queryType, files[0])
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregator: %v", err)
	}

	// Aggregate and format all files
	csvData := fa.aggregateAndFormat(aggregator, files)

	// Return as single result (partition number doesn't matter when aggregated)
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
