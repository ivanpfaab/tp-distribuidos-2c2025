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

// Query3Grouper groups Query 3 records by store_id
type Query3Grouper struct {
	year     string
	semester string
}

// NewQuery3Grouper creates a new Query 3 grouper with the specified year and semester
func NewQuery3Grouper(year, semester string) *Query3Grouper {
	return &Query3Grouper{year: year, semester: semester}
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

