package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

// GetClientFiles returns all JSON files for a given client
func (fp *FileProcessor) GetClientFiles(clientID string) ([]string, error) {
	clientDir := filepath.Join(fp.baseDir, clientID)

	// Check if client directory exists
	if _, err := os.Stat(clientDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("client directory does not exist: %s", clientDir)
	}

	// Read all files in the client directory
	entries, err := os.ReadDir(clientDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read client directory %s: %v", clientDir, err)
	}

	// Collect all .json files
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			files = append(files, filepath.Join(clientDir, entry.Name()))
		}
	}

	// Sort files for consistent ordering
	sort.Strings(files)

	testing_utils.LogInfo("FileProcessor", "Found %d files for client %s", len(files), clientID)
	return files, nil
}

// ReadAndConvertFile reads a JSON file and converts it to CSV format based on query type
func (fp *FileProcessor) ReadAndConvertFile(filePath string) (string, error) {
	// Read the JSON file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %v", filePath, err)
	}

	// Parse JSON
	var jsonData map[string]interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		return "", fmt.Errorf("failed to unmarshal JSON from %s: %v", filePath, err)
	}

	// Extract year and semester/month from filename for Q2 and Q3
	fileName := filepath.Base(filePath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, ".json")

	// Convert to CSV based on query type
	switch fp.queryType {
	case 2:
		return fp.convertQuery2ToCSV(jsonData, fileNameWithoutExt)
	case 3:
		return fp.convertQuery3ToCSV(jsonData, fileNameWithoutExt)
	case 4:
		return fp.convertQuery4ToCSV(jsonData)
	default:
		return "", fmt.Errorf("unsupported query type: %d", fp.queryType)
	}
}

// convertQuery2ToCSV converts Query 2 grouped data to CSV
// Expected output schema: year,month,item_id,quantity,subtotal,count
// Input file structure: { "month": { "item_id": { "total_quantity": int, "total_subtotal": float, "count": int } } }
// File name format: YYYY-S.json (e.g., 2024-1.json for S1-2024)
func (fp *FileProcessor) convertQuery2ToCSV(jsonData map[string]interface{}, fileName string) (string, error) {
	// Parse year and semester from filename (e.g., "2024-1" -> year=2024, semester=1)
	parts := strings.Split(fileName, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid filename format: %s", fileName)
	}
	year := parts[0]
	semester := parts[1]

	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("year,month,item_id,quantity,subtotal,count\n")

	// Sort months for consistent ordering
	months := make([]string, 0, len(jsonData))
	for month := range jsonData {
		months = append(months, month)
	}
	sort.Strings(months)

	// Iterate through each month
	for _, month := range months {
		monthData, ok := jsonData[month].(map[string]interface{})
		if !ok {
			testing_utils.LogWarn("FileProcessor", "Invalid month data for month %s", month)
			continue
		}

		// Sort item IDs for consistent ordering
		itemIDs := make([]string, 0, len(monthData))
		for itemID := range monthData {
			itemIDs = append(itemIDs, itemID)
		}
		sort.Strings(itemIDs)

		// Iterate through each item
		for _, itemID := range itemIDs {
			itemData, ok := monthData[itemID].(map[string]interface{})
			if !ok {
				testing_utils.LogWarn("FileProcessor", "Invalid item data for item %s", itemID)
				continue
			}

			// Extract fields
			quantity := toInt(itemData["total_quantity"])
			subtotal := toFloat64(itemData["total_subtotal"])
			count := toInt(itemData["count"])

			// Determine the actual year-month based on semester and month
			// Semester 1 (1) = Jan-Jun (months 1-6)
			// Semester 2 (2) = Jul-Dec (months 7-12)
			actualMonth := month
			if semester == "1" {
				// S1: months 1-6
				actualMonth = month
			} else if semester == "2" {
				// S2: months 7-12, but stored as 7-12
				actualMonth = month
			}

			// Format: year,month,item_id,quantity,subtotal,count
			csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%d,%.2f,%d\n",
				year, actualMonth, itemID, quantity, subtotal, count))
		}
	}

	return csvBuilder.String(), nil
}

// convertQuery3ToCSV converts Query 3 grouped data to CSV
// Expected output schema: year,semester,store_id,total_final_amount,count
// Input file structure: { "store_id": { "total_final_amount": float, "count": int } }
// File name format: YYYY-S.json (e.g., 2024-1.json for S1-2024)
func (fp *FileProcessor) convertQuery3ToCSV(jsonData map[string]interface{}, fileName string) (string, error) {
	// Parse year and semester from filename (e.g., "2024-1" -> year=2024, semester=1)
	parts := strings.Split(fileName, "-")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid filename format: %s", fileName)
	}
	year := parts[0]
	semester := parts[1]

	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("year,semester,store_id,total_final_amount,count\n")

	// Sort store IDs for consistent ordering
	storeIDs := make([]string, 0, len(jsonData))
	for storeID := range jsonData {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Strings(storeIDs)

	// Iterate through each store
	for _, storeID := range storeIDs {
		storeData, ok := jsonData[storeID].(map[string]interface{})
		if !ok {
			testing_utils.LogWarn("FileProcessor", "Invalid store data for store %s", storeID)
			continue
		}

		// Extract fields
		totalFinalAmount := toFloat64(storeData["total_final_amount"])
		count := toInt(storeData["count"])

		// Format: year,semester,store_id,total_final_amount,count
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%.2f,%d\n",
			year, semester, storeID, totalFinalAmount, count))
	}

	return csvBuilder.String(), nil
}

// convertQuery4ToCSV converts Query 4 grouped data to CSV
// Expected output schema: user_id,store_id,count
// Input file structure: { "user_id|store_id": { "user_id": string, "store_id": string, "count": int } }
func (fp *FileProcessor) convertQuery4ToCSV(jsonData map[string]interface{}) (string, error) {
	var csvBuilder strings.Builder

	// Write header
	csvBuilder.WriteString("user_id,store_id,count\n")

	// Sort keys for consistent ordering
	keys := make([]string, 0, len(jsonData))
	for key := range jsonData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Iterate through each user-store combination
	for _, key := range keys {
		userData, ok := jsonData[key].(map[string]interface{})
		if !ok {
			testing_utils.LogWarn("FileProcessor", "Invalid user data for key %s", key)
			continue
		}

		// Extract fields
		userID, _ := userData["user_id"].(string)
		storeID, _ := userData["store_id"].(string)
		count := toInt(userData["count"])

		// Format: user_id,store_id,count
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d\n", userID, storeID, count))
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

// DeleteClientDirectory deletes the entire client directory
func (fp *FileProcessor) DeleteClientDirectory(clientID string) error {
	clientDir := filepath.Join(fp.baseDir, clientID)
	if err := os.RemoveAll(clientDir); err != nil {
		return fmt.Errorf("failed to delete client directory %s: %v", clientDir, err)
	}
	testing_utils.LogInfo("FileProcessor", "Deleted client directory: %s", clientDir)
	return nil
}

// toInt converts interface{} to int, handling both int and float64 (from JSON unmarshaling)
func toInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case float64:
		return int(v)
	case int64:
		return int(v)
	default:
		testing_utils.LogWarn("FileProcessor", "Unexpected type for int conversion: %T, defaulting to 0", val)
		return 0
	}
}

// toFloat64 converts interface{} to float64, handling both float64 and int (for consistency)
func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		testing_utils.LogWarn("FileProcessor", "Unexpected type for float64 conversion: %T, defaulting to 0.0", val)
		return 0.0
	}
}
