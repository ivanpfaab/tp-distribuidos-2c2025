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
	baseDir   string
}

// NewFileProcessor creates a new file processor for a specific query type
func NewFileProcessor(queryType int) *FileProcessor {
	baseDir := filepath.Join(GroupByDataDir, fmt.Sprintf("q%d", queryType))
	return &FileProcessor{
		queryType: queryType,
		baseDir:   baseDir,
	}
}

// GetClientFiles returns all files for a given client
// For Query 2, 3, and 4: returns CSV partition files following naming convention
func (fp *FileProcessor) GetClientFiles(clientID string) ([]string, error) {
	if fp.queryType == 2 {
		return fp.getQuery2ClientFiles(clientID)
	}
	if fp.queryType == 3 {
		return fp.getQuery3ClientFiles(clientID)
	}
	if fp.queryType == 4 {
		return fp.getQuery4ClientFiles(clientID)
	}

	return nil, fmt.Errorf("unsupported query type: %d", fp.queryType)
}

// getQuery2ClientFiles returns all CSV partition files for Query 2
// Files follow pattern: {clientID}-q2-partition-{XXX}.csv in baseDir
func (fp *FileProcessor) getQuery2ClientFiles(clientID string) ([]string, error) {
	prefix := fmt.Sprintf("%s-q2-partition-", clientID)
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

	testing_utils.LogInfo("FileProcessor", "Found %d CSV partition files for client %s (Query 2)", len(files), clientID)
	return files, nil
}

// getQuery3ClientFiles returns all CSV partition files for Query 3
// Files follow pattern: {clientID}-q3-partition-{XXX}.csv in baseDir
func (fp *FileProcessor) getQuery3ClientFiles(clientID string) ([]string, error) {
	prefix := fmt.Sprintf("%s-q3-partition-", clientID)
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

	testing_utils.LogInfo("FileProcessor", "Found %d CSV partition files for client %s (Query 3)", len(files), clientID)
	return files, nil
}

// getQuery4ClientFiles returns all CSV partition files for Query 4
// Files follow pattern: {clientID}-q4-partition-{XXX}.csv in baseDir
func (fp *FileProcessor) getQuery4ClientFiles(clientID string) ([]string, error) {
	// Query 4 files are in baseDir, not clientDir
	prefix := fmt.Sprintf("%s-q4-partition-", clientID)
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

	testing_utils.LogInfo("FileProcessor", "Found %d CSV partition files for client %s", len(files), clientID)
	return files, nil
}

// ReadAndConvertFile reads a file and converts it to CSV format based on query type
// For Query 2, 3, and 4: reads CSV and groups it (aggregation happens here)
func (fp *FileProcessor) ReadAndConvertFile(filePath string) (string, error) {
	switch fp.queryType {
	case 2:
		return fp.readAndGroupQuery2CSV(filePath)
	case 3:
		return fp.readAndGroupQuery3CSV(filePath)
	case 4:
		return fp.readAndGroupQuery4CSV(filePath)
	default:
		return "", fmt.Errorf("unsupported query type: %d", fp.queryType)
	}
}

// readAndGroupQuery4CSV reads a CSV partition file and groups user_id,store_id pairs
// Returns aggregated CSV with user_id,store_id,count
// Memory-efficient: groups while reading, clears after each file
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
	// Pre-allocate map with estimated capacity (can be large for Q4)
	grouped := make(map[string]int, 10000)

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
	var files []string
	var err error

	switch fp.queryType {
	case 2:
		files, err = fp.getQuery2ClientFiles(clientID)
	case 3:
		files, err = fp.getQuery3ClientFiles(clientID)
	case 4:
		files, err = fp.getQuery4ClientFiles(clientID)
	default:
		return fmt.Errorf("unsupported query type: %d", fp.queryType)
	}

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
