package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
)

const (
	GroupByDataDir = "/app/groupby-data"
)

// FileProcessor handles reading and converting grouped data files
type FileProcessor struct {
	queryType int
	workerID  int
	baseDir   string
}

// NewFileProcessor creates a new file processor for a specific query type and worker
func NewFileProcessor(queryType int, workerID int) *FileProcessor {
	// Each orchestrator reads from its worker's volume: /app/groupby-data/q{queryType}/worker-{id}/
	baseDir := filepath.Join(GroupByDataDir, fmt.Sprintf("q%d", queryType), fmt.Sprintf("worker-%d", workerID))
	return &FileProcessor{
		queryType: queryType,
		workerID:  workerID,
		baseDir:   baseDir,
	}
}

// GetClientFiles returns all files for a given client
// For Query 2, 3, and 4: returns CSV partition files following naming convention
// Files follow pattern: {clientID}-q{queryType}-partition-{XXX}.csv in baseDir
func (fp *FileProcessor) GetClientFiles(clientID string) ([]string, error) {
	// Validate query type
	if fp.queryType < 2 || fp.queryType > 4 {
		return nil, fmt.Errorf("unsupported query type: %d", fp.queryType)
	}

	// Build file prefix pattern: {clientID}-q{queryType}-partition-
	prefix := fmt.Sprintf("%s-q%d-partition-", clientID, fp.queryType)
	var files []string

	// Read all files in base directory
	entries, err := os.ReadDir(fp.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base directory %s: %v", fp.baseDir, err)
	}

	// Collect files matching the pattern
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".csv") {
			files = append(files, filepath.Join(fp.baseDir, entry.Name()))
		}
	}

	// Sort files for consistent ordering (by partition number)
	sort.Strings(files)

	testing_utils.LogInfo("FileProcessor", "Found %d CSV partition files for client %s (Query %d)", len(files), clientID, fp.queryType)
	return files, nil
}

// ReadAndConvertFile reads a file and converts it to CSV format based on query type
// For Query 2, 3, and 4: reads CSV and groups it (aggregation happens here)
func (fp *FileProcessor) ReadAndConvertFile(filePath string) (string, error) {
	var grouper RecordGrouper

	switch fp.queryType {
	case 2:
		// Extract partition from filename to get year
		partition, err := fp.extractPartitionFromFilename(filePath)
		if err != nil {
			return "", err
		}
		year := fp.getYearFromPartition(partition)
		grouper = &Query2Grouper{year: year}
	case 3:
		// Extract partition from filename to get year and semester
		partition, err := fp.extractPartitionFromFilename(filePath)
		if err != nil {
			return "", err
		}
		year, semester := fp.getYearSemesterFromPartition(partition)
		grouper = &Query3Grouper{year: year, semester: semester}
	case 4:
		grouper = &Query4Grouper{}
	default:
		return "", fmt.Errorf("unsupported query type: %d", fp.queryType)
	}

	return fp.readAndGroupCSV(filePath, grouper)
}

// extractPartitionFromFilename extracts partition number from filename
// Filename format: {clientID}-q{queryType}-partition-{XXX}.csv
func (fp *FileProcessor) extractPartitionFromFilename(filePath string) (int, error) {
	fileName := filepath.Base(filePath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, ".csv")

	// Extract partition number (last part after "partition-")
	parts := strings.Split(fileNameWithoutExt, "-partition-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid filename format: %s", fileName)
	}

	partitionStr := parts[1]
	var partition int
	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		return 0, fmt.Errorf("failed to parse partition number from filename %s: %v", fileName, err)
	}

	return partition, nil
}

// getYearFromPartition returns year string from partition number
// Partition 0 = 2024-S1, Partition 1 = 2024-S2, Partition 2 = 2025-S1
func (fp *FileProcessor) getYearFromPartition(partition int) string {
	switch partition {
	case 0, 1:
		return "2024"
	case 2:
		return "2025"
	default:
		return "2024" // Fallback
	}
}

// getYearSemesterFromPartition returns year and semester strings from partition number
// Partition 0 = 2024-S1, Partition 1 = 2024-S2, Partition 2 = 2025-S1
func (fp *FileProcessor) getYearSemesterFromPartition(partition int) (string, string) {
	switch partition {
	case 0:
		return "2024", "1"
	case 1:
		return "2024", "2"
	case 2:
		return "2025", "1"
	default:
		return "2024", "1" // Fallback
	}
}

// readAndGroupCSV is a generic method that reads and groups CSV files using a RecordGrouper
func (fp *FileProcessor) readAndGroupCSV(filePath string, grouper RecordGrouper) (string, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create buffered CSV reader for better I/O performance
	bufferedFile := bufio.NewReaderSize(file, 64*1024) // 64KB buffer
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true // Reuse record slice to reduce allocations

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return grouper.GetHeader(), nil // Empty file, return header only
		}
		return "", fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Initialize grouped data based on query type
	var groupedData map[string]interface{}
	switch grouper.(type) {
	case *Query2Grouper:
		// Query 2: map[month|item_id] -> *Query2AggregatedData
		groupedData = make(map[string]interface{}, 500)
	case *Query3Grouper:
		// Query 3: map[store_id] -> *Query3AggregatedData
		groupedData = make(map[string]interface{}, 20)
	case *Query4Grouper:
		// Query 4: map[user_id|store_id] -> count (int)
		groupedData = make(map[string]interface{}) // Start empty, let Go grow as needed
	}

	// Stream records one at a time and group them
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			// Other error - log and continue
			testing_utils.LogWarn("FileProcessor", "Error reading from %s: %v", filePath, err)
			break
		}

		if len(record) < grouper.GetMinFieldCount() {
			testing_utils.LogWarn("FileProcessor", "Skipping malformed record in %s: %v", filePath, record)
			continue
		}

		// Process record using the grouper
		key, shouldContinue, err := grouper.ProcessRecord(record)
		if err != nil {
			testing_utils.LogWarn("FileProcessor", "Skipping record in %s: %v", filePath, err)
			continue
		}
		if !shouldContinue {
			continue // Grouper decided to skip this record
		}

		// Aggregate based on query type
		switch grouper.(type) {
		case *Query2Grouper:
			// Parse quantity and subtotal
			quantity, _ := strconv.Atoi(strings.TrimSpace(record[2]))
			subtotal, _ := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)

			// Get or create aggregated data
			if groupedData[key] == nil {
				groupedData[key] = &Query2AggregatedData{}
			}
			agg := groupedData[key].(*Query2AggregatedData)
			agg.TotalQuantity += quantity
			agg.TotalSubtotal += subtotal
			agg.Count++

		case *Query3Grouper:
			// Parse final_amount
			finalAmount, _ := strconv.ParseFloat(strings.TrimSpace(record[1]), 64)

			// Get or create aggregated data
			if groupedData[key] == nil {
				groupedData[key] = &Query3AggregatedData{}
			}
			agg := groupedData[key].(*Query3AggregatedData)
			agg.TotalFinalAmount += finalAmount
			agg.Count++

		case *Query4Grouper:
			// Simple count aggregation
			if count, ok := groupedData[key].(int); ok {
				groupedData[key] = count + 1
			} else {
				groupedData[key] = 1
			}
		}
	}

	// Format output using the grouper
	result := grouper.FormatOutput(groupedData)

	// Clear the map to free memory immediately after formatting
	for k := range groupedData {
		delete(groupedData, k)
	}
	groupedData = nil

	return result, nil
}

// readAndGroupQuery4CSV reads a CSV partition file and groups user_id,store_id pairs
// Returns aggregated CSV with user_id,store_id,count
// Memory-efficient: groups while reading, clears after each file
// DEPRECATED: Use readAndGroupCSV with Query4Grouper instead
func (fp *FileProcessor) readAndGroupQuery4CSV(filePath string) (string, error) {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create buffered CSV reader for better I/O performance
	bufferedFile := bufio.NewReaderSize(file, 64*1024) // 64KB buffer
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true // Reuse record slice to reduce allocations

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return "user_id,store_id,count\n", nil // Empty file, return header only
		}
		return "", fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Group records: map[user_id|store_id] -> count
	// Start empty, let Go grow as needed
	grouped := make(map[string]int)

	// Stream records one at a time instead of loading all into memory
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			// Other error - log and continue
			testing_utils.LogWarn("FileProcessor", "Error reading from %s: %v", filePath, err)
			break
		}
		if len(record) < 2 {
			testing_utils.LogWarn("FileProcessor", "Skipping malformed record in %s: %v", filePath, record)
			continue
		}

		userID := strings.TrimSpace(record[0])
		storeID := strings.TrimSpace(record[1])

		if userID == "" || storeID == "" {
			continue
		}

		// Create composite key
		key := fmt.Sprintf("%s|%s", userID, storeID)
		grouped[key]++
	}

	// Convert grouped data to CSV
	// Pre-allocate string builder with estimated capacity (avg ~30 bytes per record)
	estimatedSize := len(grouped) * 30
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString("user_id,store_id,count\n")

	// Sort keys for consistent ordering
	// Pre-allocate slice with known capacity
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Write aggregated records
	for _, key := range keys {
		parts := strings.Split(key, "|")
		if len(parts) != 2 {
			continue
		}
		userID := parts[0]
		storeID := parts[1]
		count := grouped[key]

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d\n", userID, storeID, count))
	}

	return csvBuilder.String(), nil
}

// readAndGroupQuery2CSV reads a CSV partition file and groups month,item_id pairs
// Returns aggregated CSV with year,month,item_id,quantity,subtotal,count
// Memory-efficient: groups while reading, clears after each file
func (fp *FileProcessor) readAndGroupQuery2CSV(filePath string) (string, error) {
	// Extract partition number from filename to determine year
	// Filename format: {clientID}-q2-partition-{XXX}.csv
	fileName := filepath.Base(filePath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, ".csv")

	// Extract partition number (last part after "partition-")
	parts := strings.Split(fileNameWithoutExt, "-partition-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid filename format for Query 2: %s", fileName)
	}

	partitionStr := parts[1]
	var partition int
	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		return "", fmt.Errorf("failed to parse partition number from filename %s: %v", fileName, err)
	}

	// Determine year from partition
	// Partition 0 = 2024-S1, Partition 1 = 2024-S2, Partition 2 = 2025-S1
	var year string
	switch partition {
	case 0:
		year = "2024"
	case 1:
		year = "2024"
	case 2:
		year = "2025"
	default:
		year = "2024" // Fallback
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create buffered CSV reader for better I/O performance
	bufferedFile := bufio.NewReaderSize(file, 64*1024) // 64KB buffer
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true // Reuse record slice to reduce allocations

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return "year,month,item_id,quantity,subtotal,count\n", nil // Empty file, return header only
		}
		return "", fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Group records: map[month|item_id] -> {total_quantity, total_subtotal, count}
	type AggregatedData struct {
		TotalQuantity int
		TotalSubtotal float64
		Count         int
	}
	// Pre-allocate map with estimated capacity (typically 100-1000 unique combinations)
	grouped := make(map[string]*AggregatedData, 500)

	// Stream records one at a time instead of loading all into memory
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			// Other error - log and continue
			testing_utils.LogWarn("FileProcessor", "Error reading from %s: %v", filePath, err)
			break
		}
		if len(record) < 4 {
			testing_utils.LogWarn("FileProcessor", "Skipping malformed record in %s: %v", filePath, record)
			continue
		}

		month := strings.TrimSpace(record[0])
		itemID := strings.TrimSpace(record[1])
		quantityStr := strings.TrimSpace(record[2])
		subtotalStr := strings.TrimSpace(record[3])

		if month == "" || itemID == "" || quantityStr == "" || subtotalStr == "" {
			continue
		}

		// Parse quantity and subtotal
		quantity, err := strconv.Atoi(quantityStr)
		if err != nil {
			testing_utils.LogWarn("FileProcessor", "Skipping record with invalid quantity in %s: %v", filePath, err)
			continue
		}

		subtotal, err := strconv.ParseFloat(subtotalStr, 64)
		if err != nil {
			testing_utils.LogWarn("FileProcessor", "Skipping record with invalid subtotal in %s: %v", filePath, err)
			continue
		}

		// Create composite key
		key := fmt.Sprintf("%s|%s", month, itemID)

		// Aggregate
		if grouped[key] == nil {
			grouped[key] = &AggregatedData{}
		}
		grouped[key].TotalQuantity += quantity
		grouped[key].TotalSubtotal += subtotal
		grouped[key].Count++
	}

	// Convert grouped data to CSV
	// Pre-allocate string builder with estimated capacity (avg ~50 bytes per record)
	estimatedSize := len(grouped) * 50
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString("year,month,item_id,quantity,subtotal,count\n")

	// Sort keys for consistent ordering
	// Pre-allocate slice with known capacity
	keys := make([]string, 0, len(grouped))
	for key := range grouped {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Write aggregated records
	for _, key := range keys {
		parts := strings.Split(key, "|")
		if len(parts) != 2 {
			continue
		}
		month := parts[0]
		itemID := parts[1]
		agg := grouped[key]

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%d,%.2f,%d\n",
			year, month, itemID, agg.TotalQuantity, agg.TotalSubtotal, agg.Count))
	}

	return csvBuilder.String(), nil
}

// readAndGroupQuery3CSV reads a CSV partition file and groups store_id
// Returns aggregated CSV with year,semester,store_id,total_final_amount,count
// Memory-efficient: groups while reading, clears after each file
func (fp *FileProcessor) readAndGroupQuery3CSV(filePath string) (string, error) {
	// Extract partition number from filename to determine year and semester
	// Filename format: {clientID}-q3-partition-{XXX}.csv
	fileName := filepath.Base(filePath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, ".csv")

	// Extract partition number (last part after "partition-")
	parts := strings.Split(fileNameWithoutExt, "-partition-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid filename format for Query 3: %s", fileName)
	}

	partitionStr := parts[1]
	var partition int
	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		return "", fmt.Errorf("failed to parse partition number from filename %s: %v", fileName, err)
	}

	// Determine year and semester from partition
	// Partition 0 = 2024-S1, Partition 1 = 2024-S2, Partition 2 = 2025-S1
	var year, semester string
	switch partition {
	case 0:
		year = "2024"
		semester = "1"
	case 1:
		year = "2024"
		semester = "2"
	case 2:
		year = "2025"
		semester = "1"
	default:
		year = "2024"  // Fallback
		semester = "1" // Fallback
	}

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open file %s: %v", filePath, err)
	}
	defer file.Close()

	// Create buffered CSV reader for better I/O performance
	bufferedFile := bufio.NewReaderSize(file, 64*1024) // 64KB buffer
	reader := csv.NewReader(bufferedFile)
	reader.ReuseRecord = true // Reuse record slice to reduce allocations

	// Skip header row
	_, err = reader.Read()
	if err != nil {
		if err == io.EOF {
			return "year,semester,store_id,total_final_amount,count\n", nil // Empty file, return header only
		}
		return "", fmt.Errorf("failed to read header from %s: %v", filePath, err)
	}

	// Group records: map[store_id] -> {total_final_amount, count}
	type AggregatedData struct {
		TotalFinalAmount float64
		Count            int
	}
	// Pre-allocate map with estimated capacity (typically 10-50 stores)
	grouped := make(map[string]*AggregatedData, 20)

	// Stream records one at a time instead of loading all into memory
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // Normal end of file
			}
			// Other error - log and continue
			testing_utils.LogWarn("FileProcessor", "Error reading from %s: %v", filePath, err)
			break
		}
		if len(record) < 2 {
			testing_utils.LogWarn("FileProcessor", "Skipping malformed record in %s: %v", filePath, record)
			continue
		}

		storeID := strings.TrimSpace(record[0])
		finalAmountStr := strings.TrimSpace(record[1])

		if storeID == "" || finalAmountStr == "" {
			continue
		}

		// Parse final_amount
		finalAmount, err := strconv.ParseFloat(finalAmountStr, 64)
		if err != nil {
			testing_utils.LogWarn("FileProcessor", "Skipping record with invalid final_amount in %s: %v", filePath, err)
			continue
		}

		// Aggregate
		if grouped[storeID] == nil {
			grouped[storeID] = &AggregatedData{}
		}
		grouped[storeID].TotalFinalAmount += finalAmount
		grouped[storeID].Count++
	}

	// Convert grouped data to CSV
	// Pre-allocate string builder with estimated capacity (avg ~60 bytes per record)
	estimatedSize := len(grouped) * 60
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString("year,semester,store_id,total_final_amount,count\n")

	// Sort store IDs for consistent ordering
	// Pre-allocate slice with known capacity
	storeIDs := make([]string, 0, len(grouped))
	for storeID := range grouped {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Strings(storeIDs)

	// Write aggregated records
	for _, storeID := range storeIDs {
		agg := grouped[storeID]
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%.2f,%d\n",
			year, semester, storeID, agg.TotalFinalAmount, agg.Count))
	}

	return csvBuilder.String(), nil
}

// DeleteFile deletes a file from the filesystem
func (fp *FileProcessor) DeleteFile(filePath string) error {
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete file %s: %v", filePath, err)
	}
	testing_utils.LogInfo("FileProcessor", "Deleted file: %s", filePath)
	return nil
}

// DeleteClientDirectory deletes partition CSV files for Query 2, 3, and 4
func (fp *FileProcessor) DeleteClientDirectory(clientID string) error {
	// Get all files for this client using the consolidated method
	files, err := fp.GetClientFiles(clientID)
	if err != nil {
		return fmt.Errorf("failed to get files for deletion: %v", err)
	}

	for _, file := range files {
		if err := fp.DeleteFile(file); err != nil {
			testing_utils.LogWarn("FileProcessor", "Failed to delete file %s: %v", file, err)
			// Continue deleting other files
		}
	}

	testing_utils.LogInfo("FileProcessor", "Deleted %d partition files for client %s (Query %d)", len(files), clientID, fp.queryType)
	return nil
}
