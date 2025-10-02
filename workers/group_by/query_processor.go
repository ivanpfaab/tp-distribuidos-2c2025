package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// QueryType2Result represents the result of QueryType 2 (transaction_items by year, month, item_id)
type QueryType2Result struct {
	Year     int
	Month    int
	ItemID   int
	Quantity int
	Subtotal float64
	Count    int
}

// QueryType3Result represents the result of QueryType 3 (transactions by year, semester, store_id)
type QueryType3Result struct {
	Year             int
	Semester         int
	StoreID          int
	TotalFinalAmount float64
	Count            int
}

// QueryType4Result represents the result of QueryType 4 (transactions by user_id, store_id)
type QueryType4Result struct {
	UserID  int
	StoreID int
	Count   int
}

// QueryProcessor handles different types of group by queries
type QueryProcessor struct{}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor() *QueryProcessor {
	return &QueryProcessor{}
}

// ProcessQuery processes a chunk based on the query type and data schema
func (qp *QueryProcessor) ProcessQuery(chunkData *chunk.Chunk, dataType DataSchema) interface{} {
	switch chunkData.QueryType {
	case 1:
		// QueryType 1 has no group by - return empty result
		fmt.Printf("QueryType 1 has no group by operation, returning empty result\n")
		return []QueryType2Result{}
	case 2:
		return qp.processQueryType2(chunkData, dataType)
	case 3:
		return qp.processQueryType3(chunkData, dataType)
	case 4:
		return qp.processQueryType4(chunkData, dataType)
	default:
		fmt.Printf("Unknown query type: %d, returning empty result\n", chunkData.QueryType)
		return []QueryType2Result{} // TODO handle more nicely
	}
}

// processQueryType2 groups transaction_items by year, month, and item_id
func (qp *QueryProcessor) processQueryType2(chunkData *chunk.Chunk, dataType DataSchema) []QueryType2Result {
	if dataType == RawData {
		fmt.Printf("Processing Query Type 2 (RAW DATA) - Group by year, month, item_id\n")
		return qp.processQueryType2Raw(chunkData)
	} else {
		fmt.Printf("Processing Query Type 2 (GROUPED DATA) - Aggregate grouped results\n")
		return qp.processQueryType2Grouped(chunkData)
	}
}

// processQueryType3 groups transactions by year, semester, and store_id
func (qp *QueryProcessor) processQueryType3(chunkData *chunk.Chunk, dataType DataSchema) []QueryType3Result {
	if dataType == RawData {
		fmt.Printf("Processing Query Type 3 (RAW DATA) - Group by year, semester, store_id\n")
		return qp.processQueryType3Raw(chunkData)
	} else {
		fmt.Printf("Processing Query Type 3 (GROUPED DATA) - Aggregate grouped results\n")
		return qp.processQueryType3Grouped(chunkData)
	}
}

// processQueryType4 groups transactions by user_id and store_id
func (qp *QueryProcessor) processQueryType4(chunkData *chunk.Chunk, dataType DataSchema) []QueryType4Result {
	if dataType == RawData {
		fmt.Printf("Processing Query Type 4 (RAW DATA) - Group by user_id, store_id\n")
		return qp.processQueryType4Raw(chunkData)
	} else {
		fmt.Printf("Processing Query Type 4 (GROUPED DATA) - Aggregate grouped results\n")
		return qp.processQueryType4Grouped(chunkData)
	}
}

// processQueryType2Raw processes raw transaction_items data
func (qp *QueryProcessor) processQueryType2Raw(chunkData *chunk.Chunk) []QueryType2Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return []QueryType2Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType2Result{}
	}

	// Get schema for raw data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), RawData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType2Result{}
		}
	}

	// Group by year, month, item_id
	groupMap := make(map[string]*QueryType2Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["created_at"] {
			continue
		}

		// Parse using schema-driven column mapping
		itemID, err := strconv.Atoi(record[colMap["item_id"]])
		if err != nil {
			continue
		}

		quantity, err := strconv.Atoi(record[colMap["quantity"]])
		if err != nil {
			continue
		}

		subtotal, err := strconv.ParseFloat(record[colMap["subtotal"]], 64)
		if err != nil {
			continue
		}

		// Parse created_at to extract year and month
		createdAt, err := time.Parse("2006-01-02 15:04:05", record[colMap["created_at"]])
		if err != nil {
			continue
		}

		year := createdAt.Year()
		month := int(createdAt.Month())

		// Create group key
		groupKey := fmt.Sprintf("%d-%02d-%d", year, month, itemID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.Quantity += quantity
			group.Subtotal += subtotal
			group.Count++
		} else {
			groupMap[groupKey] = &QueryType2Result{
				Year:     year,
				Month:    month,
				ItemID:   itemID,
				Quantity: quantity,
				Subtotal: subtotal,
				Count:    1,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType2Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 2 (RAW) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processQueryType2Grouped processes already-grouped transaction_items data
func (qp *QueryProcessor) processQueryType2Grouped(chunkData *chunk.Chunk) []QueryType2Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing grouped CSV: %v\n", err)
		return []QueryType2Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType2Result{}
	}

	// Get schema for grouped data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), GroupedData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType2Result{}
		}
	}

	// Group by year, month, item_id (aggregate already grouped data)
	groupMap := make(map[string]*QueryType2Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["count"] {
			continue
		}

		// Parse using schema-driven column mapping
		year, err := strconv.Atoi(record[colMap["year"]])
		if err != nil {
			continue
		}

		month, err := strconv.Atoi(record[colMap["month"]])
		if err != nil {
			continue
		}

		itemID, err := strconv.Atoi(record[colMap["item_id"]])
		if err != nil {
			continue
		}

		quantity, err := strconv.Atoi(record[colMap["quantity"]])
		if err != nil {
			continue
		}

		subtotal, err := strconv.ParseFloat(record[colMap["subtotal"]], 64)
		if err != nil {
			continue
		}

		count, err := strconv.Atoi(record[colMap["count"]])
		if err != nil {
			continue
		}

		// Create group key
		groupKey := fmt.Sprintf("%d-%02d-%d", year, month, itemID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.Quantity += quantity
			group.Subtotal += subtotal
			group.Count += count
		} else {
			groupMap[groupKey] = &QueryType2Result{
				Year:     year,
				Month:    month,
				ItemID:   itemID,
				Quantity: quantity,
				Subtotal: subtotal,
				Count:    count,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType2Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 2 (GROUPED) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processQueryType3Raw processes raw transactions data for QueryType 3
func (qp *QueryProcessor) processQueryType3Raw(chunkData *chunk.Chunk) []QueryType3Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return []QueryType3Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType3Result{}
	}

	// Get schema for raw data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), RawData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType3Result{}
		}
	}

	// Group by year, semester, store_id
	groupMap := make(map[string]*QueryType3Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["created_at"] {
			continue
		}

		// Parse using schema-driven column mapping
		storeID, err := strconv.Atoi(record[colMap["store_id"]])
		if err != nil {
			continue
		}

		finalAmount, err := strconv.ParseFloat(record[colMap["final_amount"]], 64)
		if err != nil {
			continue
		}

		// Parse created_at to extract year and semester
		createdAt, err := time.Parse("2006-01-02 15:04:05", record[colMap["created_at"]])
		if err != nil {
			continue
		}

		year := createdAt.Year()
		month := int(createdAt.Month())
		semester := 1
		if month >= 7 {
			semester = 2
		}

		// Create group key
		groupKey := fmt.Sprintf("%d-%d-%d", year, semester, storeID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.TotalFinalAmount += finalAmount
			group.Count++
		} else {
			groupMap[groupKey] = &QueryType3Result{
				Year:             year,
				Semester:         semester,
				StoreID:          storeID,
				TotalFinalAmount: finalAmount,
				Count:            1,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType3Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 3 (RAW) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processQueryType3Grouped processes already-grouped transactions data for QueryType 3
func (qp *QueryProcessor) processQueryType3Grouped(chunkData *chunk.Chunk) []QueryType3Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing grouped CSV: %v\n", err)
		return []QueryType3Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType3Result{}
	}

	// Get schema for grouped data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), GroupedData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType3Result{}
		}
	}

	// Group by year, semester, store_id (aggregate already grouped data)
	groupMap := make(map[string]*QueryType3Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["count"] {
			continue
		}

		// Parse using schema-driven column mapping
		year, err := strconv.Atoi(record[colMap["year"]])
		if err != nil {
			continue
		}

		semester, err := strconv.Atoi(record[colMap["semester"]])
		if err != nil {
			continue
		}

		storeID, err := strconv.Atoi(record[colMap["store_id"]])
		if err != nil {
			continue
		}

		totalFinalAmount, err := strconv.ParseFloat(record[colMap["total_final_amount"]], 64)
		if err != nil {
			continue
		}

		count, err := strconv.Atoi(record[colMap["count"]])
		if err != nil {
			continue
		}

		// Create group key
		groupKey := fmt.Sprintf("%d-%d-%d", year, semester, storeID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.TotalFinalAmount += totalFinalAmount
			group.Count += count
		} else {
			groupMap[groupKey] = &QueryType3Result{
				Year:             year,
				Semester:         semester,
				StoreID:          storeID,
				TotalFinalAmount: totalFinalAmount,
				Count:            count,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType3Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 3 (GROUPED) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processQueryType4Raw processes raw transactions data for QueryType 4
func (qp *QueryProcessor) processQueryType4Raw(chunkData *chunk.Chunk) []QueryType4Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return []QueryType4Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType4Result{}
	}

	// Get schema for raw data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), RawData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType4Result{}
		}
	}

	// Group by user_id, store_id
	groupMap := make(map[string]*QueryType4Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["store_id"] {
			continue
		}

		// Parse using schema-driven column mapping
		userID, err := strconv.Atoi(record[colMap["user_id"]])
		if err != nil {
			continue
		}

		storeID, err := strconv.Atoi(record[colMap["store_id"]])
		if err != nil {
			continue
		}

		// Create group key
		groupKey := fmt.Sprintf("%d-%d", userID, storeID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.Count++
		} else {
			groupMap[groupKey] = &QueryType4Result{
				UserID:  userID,
				StoreID: storeID,
				Count:   1,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType4Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 4 (RAW) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processQueryType4Grouped processes already-grouped transactions data for QueryType 4
func (qp *QueryProcessor) processQueryType4Grouped(chunkData *chunk.Chunk) []QueryType4Result {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing grouped CSV: %v\n", err)
		return []QueryType4Result{}
	}

	// Skip header row
	if len(records) < 2 {
		return []QueryType4Result{}
	}

	// Get schema for grouped data
	expectedSchema := GetSchemaForQueryType(int(chunkData.QueryType), GroupedData)
	header := records[0]

	// Create column index map
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	// Validate required columns exist using schema
	for _, col := range expectedSchema {
		if _, exists := colMap[col]; !exists {
			fmt.Printf("Missing required column: %s\n", col)
			return []QueryType4Result{}
		}
	}

	// Group by user_id, store_id (aggregate already grouped data)
	groupMap := make(map[string]*QueryType4Result)

	for _, record := range records[1:] { // Skip header
		if len(record) <= colMap["count"] {
			continue
		}

		// Parse using schema-driven column mapping
		userID, err := strconv.Atoi(record[colMap["user_id"]])
		if err != nil {
			continue
		}

		storeID, err := strconv.Atoi(record[colMap["store_id"]])
		if err != nil {
			continue
		}

		count, err := strconv.Atoi(record[colMap["count"]])
		if err != nil {
			continue
		}

		// Create group key
		groupKey := fmt.Sprintf("%d-%d", userID, storeID)

		// Update or create group
		if group, exists := groupMap[groupKey]; exists {
			group.Count += count
		} else {
			groupMap[groupKey] = &QueryType4Result{
				UserID:  userID,
				StoreID: storeID,
				Count:   count,
			}
		}
	}

	// Convert map to slice
	results := make([]QueryType4Result, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 4 (GROUPED) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// ResultsToCSV converts group by results to CSV format using schema
func (qp *QueryProcessor) ResultsToCSV(results interface{}, queryType int) string {
	switch queryType {
	case 2:
		return qp.resultsToCSVQueryType2(results.([]QueryType2Result))
	case 3:
		return qp.resultsToCSVQueryType3(results.([]QueryType3Result))
	case 4:
		return qp.resultsToCSVQueryType4(results.([]QueryType4Result))
	default:
		return ""
	}
}

// resultsToCSVQueryType2 converts QueryType2 results to CSV
func (qp *QueryProcessor) resultsToCSVQueryType2(results []QueryType2Result) string {
	if len(results) == 0 {
		return ""
	}

	// Get grouped schema for CSV header
	schema := GetSchemaForQueryType(2, GroupedData)

	var csv strings.Builder
	// Write header using schema
	csv.WriteString(strings.Join(schema, ","))
	csv.WriteString("\n")

	// Write data rows
	for _, result := range results {
		csv.WriteString(fmt.Sprintf("%d,%d,%d,%d,%.2f,%d\n",
			result.Year, result.Month, result.ItemID, result.Quantity, result.Subtotal, result.Count))
	}

	return csv.String()
}

// resultsToCSVQueryType3 converts QueryType3 results to CSV
func (qp *QueryProcessor) resultsToCSVQueryType3(results []QueryType3Result) string {
	if len(results) == 0 {
		return ""
	}

	// Get grouped schema for CSV header
	schema := GetSchemaForQueryType(3, GroupedData)

	var csv strings.Builder
	// Write header using schema
	csv.WriteString(strings.Join(schema, ","))
	csv.WriteString("\n")

	// Write data rows
	for _, result := range results {
		csv.WriteString(fmt.Sprintf("%d,%d,%d,%.2f,%d\n",
			result.Year, result.Semester, result.StoreID, result.TotalFinalAmount, result.Count))
	}

	return csv.String()
}

// resultsToCSVQueryType4 converts QueryType4 results to CSV
func (qp *QueryProcessor) resultsToCSVQueryType4(results []QueryType4Result) string {
	if len(results) == 0 {
		return ""
	}

	// Get grouped schema for CSV header
	schema := GetSchemaForQueryType(4, GroupedData)

	var csv strings.Builder
	// Write header using schema
	csv.WriteString(strings.Join(schema, ","))
	csv.WriteString("\n")

	// Write data rows
	for _, result := range results {
		csv.WriteString(fmt.Sprintf("%d,%d,%d\n",
			result.UserID, result.StoreID, result.Count))
	}

	return csv.String()
}
