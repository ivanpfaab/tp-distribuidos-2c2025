package grouping

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Query3AggregatedData represents aggregated data for Query 3
type Query3AggregatedData struct {
	TotalFinalAmount float64
	Count            int
}

// Query3Grouper groups Query 3 records by year|semester|store_id
type Query3Grouper struct {
}

// NewQuery3Grouper creates a new Query 3 grouper
func NewQuery3Grouper() *Query3Grouper {
	return &Query3Grouper{}
}

func (g *Query3Grouper) GetMinFieldCount() int {
	return 4 // year, semester, store_id, final_amount
}

func (g *Query3Grouper) GetHeader() string {
	return "year,semester,store_id,total_final_amount,count\n"
}

func (g *Query3Grouper) ProcessRecord(record []string) (string, bool, error) {
	year := strings.TrimSpace(record[0])
	semester := strings.TrimSpace(record[1])
	storeID := strings.TrimSpace(record[2])
	finalAmountStr := strings.TrimSpace(record[3])

	if year == "" || semester == "" || storeID == "" || finalAmountStr == "" {
		return "", false, nil // Skip this record
	}

	// Validate numeric field
	if _, err := strconv.ParseFloat(finalAmountStr, 64); err != nil {
		return "", false, fmt.Errorf("invalid final_amount: %v", err)
	}

	// Create composite key including year and semester
	key := fmt.Sprintf("%s|%s|%s", year, semester, storeID)
	return key, true, nil
}

func (g *Query3Grouper) FormatOutput(groupedData map[string]interface{}) string {
	// Pre-allocate string builder
	estimatedSize := len(groupedData) * 60
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
		semester := parts[1]
		storeID := parts[2]
		agg := groupedData[key].(*Query3AggregatedData)
		csvBuilder.WriteString(fmt.Sprintf("%s,%s,%s,%.2f,%d\n",
			year, semester, storeID, agg.TotalFinalAmount, agg.Count))
	}

	return csvBuilder.String()
}

