package grouping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Query2AggregatedData represents aggregated data for Query 2
type Query2AggregatedData struct {
	TotalQuantity int
	TotalSubtotal float64
	Count         int
}

// Query2Grouper groups Query 2 records by year|month|item_id
type Query2Grouper struct {
}

// NewQuery2Grouper creates a new Query 2 grouper
func NewQuery2Grouper() *Query2Grouper {
	return &Query2Grouper{}
}

func (g *Query2Grouper) GetMinFieldCount() int {
	return 5 // year, month, item_id, quantity, subtotal
}

func (g *Query2Grouper) GetHeader() string {
	return "year,month,item_id,quantity,subtotal,count\n"
}

func (g *Query2Grouper) ProcessRecord(record []string) (string, bool, error) {
	year := strings.TrimSpace(record[0])
	month := strings.TrimSpace(record[1])
	itemID := strings.TrimSpace(record[2])
	quantityStr := strings.TrimSpace(record[3])
	subtotalStr := strings.TrimSpace(record[4])

	if year == "" || month == "" || itemID == "" || quantityStr == "" || subtotalStr == "" {
		return "", false, nil // Skip this record
	}

	// Validate numeric fields
	if _, err := strconv.Atoi(quantityStr); err != nil {
		return "", false, fmt.Errorf("invalid quantity: %v", err)
	}
	if _, err := strconv.ParseFloat(subtotalStr, 64); err != nil {
		return "", false, fmt.Errorf("invalid subtotal: %v", err)
	}

	// Create composite key including year
	key := fmt.Sprintf("%s|%s|%s", year, month, itemID)
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
		if len(parts) != 3 {
			continue
		}
		year := parts[0]
		month := parts[1]
		itemID := parts[2]
		agg := groupedData[key].(*Query2AggregatedData)

		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%d,%.2f,%d\n",
			year, month, itemID, agg.TotalQuantity, agg.TotalSubtotal, agg.Count))
	}

	return csvBuilder.String()
}

