package main

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
)

// GroupByResult represents the result of a group by operation
type GroupByResult struct {
	Year     int
	Month    int
	ItemID   int
	Quantity int
	Subtotal float64
	Count    int
}

// QueryProcessor handles different types of group by queries
type QueryProcessor struct{}

// NewQueryProcessor creates a new query processor
func NewQueryProcessor() *QueryProcessor {
	return &QueryProcessor{}
}

// ProcessQuery processes a chunk based on the query type and data schema
func (qp *QueryProcessor) ProcessQuery(chunkData *chunk.Chunk, dataType DataSchema) []GroupByResult {
	switch chunkData.QueryType {
	case 1:
		return qp.processQueryType1(chunkData, dataType)
	default:
		fmt.Printf("Unknown query type: %d, returning empty result\n", chunkData.QueryType)
		return []GroupByResult{}
	}
}

// processQueryType1 groups transaction_items by year, month, and item_id
func (qp *QueryProcessor) processQueryType1(chunkData *chunk.Chunk, dataType DataSchema) []GroupByResult {
	if dataType == RawData {
		fmt.Printf("Processing Query Type 1 (RAW DATA) - Group by year, month, item_id\n")
		return qp.processQueryType1Raw(chunkData)
	} else {
		fmt.Printf("Processing Query Type 1 (GROUPED DATA) - Aggregate grouped results\n")
		return qp.processQueryType1Grouped(chunkData)
	}
}

// processRawTransactionItems processes raw transaction_items data
func (qp *QueryProcessor) processQueryType1Raw(chunkData *chunk.Chunk) []GroupByResult {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v\n", err)
		return []GroupByResult{}
	}

	// Skip header row
	if len(records) < 2 {
		return []GroupByResult{}
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
			return []GroupByResult{}
		}
	}

	// Group by year, month, item_id
	groupMap := make(map[string]*GroupByResult)

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
			groupMap[groupKey] = &GroupByResult{
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
	results := make([]GroupByResult, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 1 (RAW) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// processGroupedTransactionItems processes already-grouped transaction_items data
func (qp *QueryProcessor) processQueryType1Grouped(chunkData *chunk.Chunk) []GroupByResult {
	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(chunkData.ChunkData))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing grouped CSV: %v\n", err)
		return []GroupByResult{}
	}

	// Skip header row
	if len(records) < 2 {
		return []GroupByResult{}
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
			return []GroupByResult{}
		}
	}

	// Group by year, month, item_id (aggregate already grouped data)
	groupMap := make(map[string]*GroupByResult)

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
			groupMap[groupKey] = &GroupByResult{
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
	results := make([]GroupByResult, 0, len(groupMap))
	for _, group := range groupMap {
		results = append(results, *group)
	}

	fmt.Printf("Query Type 1 (GROUPED) - Processed %d records, created %d groups\n", len(records)-1, len(results))
	return results
}

// AggregateResults aggregates multiple group by results
func (qp *QueryProcessor) AggregateResults(results []GroupByResult) []GroupByResult {
	if len(results) == 0 {
		return results
	}

	// Group by the same key (year, month, item_id)
	groupMap := make(map[string]*GroupByResult)

	for _, result := range results {
		groupKey := fmt.Sprintf("%d-%02d-%d", result.Year, result.Month, result.ItemID)

		if group, exists := groupMap[groupKey]; exists {
			group.Quantity += result.Quantity
			group.Subtotal += result.Subtotal
			group.Count += result.Count
		} else {
			groupMap[groupKey] = &GroupByResult{
				Year:     result.Year,
				Month:    result.Month,
				ItemID:   result.ItemID,
				Quantity: result.Quantity,
				Subtotal: result.Subtotal,
				Count:    result.Count,
			}
		}
	}

	// Convert map to slice
	aggregatedResults := make([]GroupByResult, 0, len(groupMap))
	for _, group := range groupMap {
		aggregatedResults = append(aggregatedResults, *group)
	}

	fmt.Printf("Aggregated %d results into %d groups\n", len(results), len(aggregatedResults))
	return aggregatedResults
}

// ResultsToCSV converts group by results to CSV format using schema
func (qp *QueryProcessor) ResultsToCSV(results []GroupByResult, queryType int) string {
	if len(results) == 0 {
		return ""
	}

	// Get grouped schema for CSV header
	schema := GetSchemaForQueryType(queryType, GroupedData)

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
