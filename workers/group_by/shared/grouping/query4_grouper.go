package grouping

import (
	"fmt"
	"sort"
	"strings"
)

// Query4Grouper groups Query 4 records by user_id|store_id
type Query4Grouper struct{}

// NewQuery4Grouper creates a new Query 4 grouper
func NewQuery4Grouper() *Query4Grouper {
	return &Query4Grouper{}
}

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

