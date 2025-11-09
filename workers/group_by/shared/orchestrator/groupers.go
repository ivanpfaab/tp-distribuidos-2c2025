package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RecordGrouper defines how to group and aggregate records for a specific query type
type RecordGrouper interface {
	// GetMinFieldCount returns minimum number of fields required
	GetMinFieldCount() int
	// ProcessRecord processes a single CSV record and returns a grouping key and aggregation data
	// Returns: (key, shouldContinue, error)
	ProcessRecord(record []string) (key string, shouldContinue bool, err error)
	// GetHeader returns the CSV header for the output
	GetHeader() string
	// FormatOutput formats the grouped data into CSV rows
	// groupedData: map[key] -> aggregated data (type depends on query)
	FormatOutput(groupedData map[string]interface{}) string
}

// Query2AggregatedData represents aggregated data for Query 2
type Query2AggregatedData struct {
	TotalQuantity int
	TotalSubtotal float64
	Count         int
}

// Query2Grouper groups Query 2 records by month|item_id
type Query2Grouper struct {
	year string // Extracted from partition
}

func (g *Query2Grouper) GetMinFieldCount() int {
	return 4 // month, item_id, quantity, subtotal
}

func (g *Query2Grouper) GetHeader() string {
	return "year,month,item_id,quantity,subtotal,count\n"
}

func (g *Query2Grouper) ProcessRecord(record []string) (string, bool, error) {
	month := strings.TrimSpace(record[0])
	itemID := strings.TrimSpace(record[1])
	quantityStr := strings.TrimSpace(record[2])
	subtotalStr := strings.TrimSpace(record[3])

	if month == "" || itemID == "" || quantityStr == "" || subtotalStr == "" {
		return "", false, nil // Skip this record
	}

	// Validate numeric fields
	if _, err := strconv.Atoi(quantityStr); err != nil {
		return "", false, fmt.Errorf("invalid quantity: %v", err)
	}
	if _, err := strconv.ParseFloat(subtotalStr, 64); err != nil {
		return "", false, fmt.Errorf("invalid subtotal: %v", err)
	}

	// Create composite key
	key := fmt.Sprintf("%s|%s", month, itemID)
	return key, true, nil
}

func (g *Query2Grouper) FormatOutput(groupedData map[string]interface{}) string {
	// Pre-allocate string builder
	estimatedSize := len(groupedData) * 50
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString(g.GetHeader())

	// Sort keys for consistent ordering
	keys := make([]string, 0, len(groupedData))
	for key := range groupedData {
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
		agg := groupedData[key].(*Query2AggregatedData)

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%d,%.2f,%d\n",
			g.year, month, itemID, agg.TotalQuantity, agg.TotalSubtotal, agg.Count))
	}

	return csvBuilder.String()
}

// Query3AggregatedData represents aggregated data for Query 3
type Query3AggregatedData struct {
	TotalFinalAmount float64
	Count            int
}

// Query3Grouper groups Query 3 records by store_id
type Query3Grouper struct {
	year     string
	semester string
}

func (g *Query3Grouper) GetMinFieldCount() int {
	return 2 // store_id, final_amount
}

func (g *Query3Grouper) GetHeader() string {
	return "year,semester,store_id,total_final_amount,count\n"
}

func (g *Query3Grouper) ProcessRecord(record []string) (string, bool, error) {
	storeID := strings.TrimSpace(record[0])
	finalAmountStr := strings.TrimSpace(record[1])

	if storeID == "" || finalAmountStr == "" {
		return "", false, nil // Skip this record
	}

	// Validate numeric field
	if _, err := strconv.ParseFloat(finalAmountStr, 64); err != nil {
		return "", false, fmt.Errorf("invalid final_amount: %v", err)
	}

	// Key is just store_id
	return storeID, true, nil
}

func (g *Query3Grouper) FormatOutput(groupedData map[string]interface{}) string {
	// Pre-allocate string builder
	estimatedSize := len(groupedData) * 60
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString(g.GetHeader())

	// Sort store IDs for consistent ordering
	storeIDs := make([]string, 0, len(groupedData))
	for storeID := range groupedData {
		storeIDs = append(storeIDs, storeID)
	}
	sort.Strings(storeIDs)

	// Write aggregated records
	for _, storeID := range storeIDs {
		agg := groupedData[storeID].(*Query3AggregatedData)
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%.2f,%d\n",
			g.year, g.semester, storeID, agg.TotalFinalAmount, agg.Count))
	}

	return csvBuilder.String()
}

// Query4Grouper groups Query 4 records by user_id|store_id
type Query4Grouper struct{}

func (g *Query4Grouper) GetMinFieldCount() int {
	return 2 // user_id, store_id
}

func (g *Query4Grouper) GetHeader() string {
	return "user_id,store_id,count\n"
}

func (g *Query4Grouper) ProcessRecord(record []string) (string, bool, error) {
	userID := strings.TrimSpace(record[0])
	storeID := strings.TrimSpace(record[1])

	if userID == "" || storeID == "" {
		return "", false, nil // Skip this record
	}

	// Create composite key
	key := fmt.Sprintf("%s|%s", userID, storeID)
	return key, true, nil
}

func (g *Query4Grouper) FormatOutput(groupedData map[string]interface{}) string {
	// groupedData is map[string]int (just counts)

	// Pre-allocate string builder
	estimatedSize := len(groupedData) * 30
	var csvBuilder strings.Builder
	csvBuilder.Grow(estimatedSize)
	csvBuilder.WriteString(g.GetHeader())

	// Sort keys for consistent ordering
	keys := make([]string, 0, len(groupedData))
	for key := range groupedData {
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
		count := groupedData[key].(int)

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%d\n", userID, storeID, count))
	}

	return csvBuilder.String()
}
