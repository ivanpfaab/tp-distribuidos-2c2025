package aggregation

import (
	"log"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/shared/utils"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/grouping"
)

// Query3AggregatedData represents aggregated data for Query 3
// Re-exported from grouping package for use in aggregators
type Query3AggregatedData = grouping.Query3AggregatedData

// Query3Aggregator aggregates Query 3 partition files
type Query3Aggregator struct {
	grouper *grouping.Query3Grouper
	handler *utils.CSVHandler
}

// NewQuery3Aggregator creates a new Query 3 aggregator
func NewQuery3Aggregator() (*Query3Aggregator, error) {
	grouper := grouping.NewQuery3Grouper()

	return &Query3Aggregator{
		grouper: grouper,
		handler: utils.NewCSVHandler(""), // baseDir not needed for ReadFileStreaming
	}, nil
}

// GetGrouper returns the grouper
func (a *Query3Aggregator) GetGrouper() grouping.RecordGrouper {
	return a.grouper
}

// InitializeDataMap creates and initializes the aggregated data map for Query 3
func (a *Query3Aggregator) InitializeDataMap() map[string]interface{} {
	return make(map[string]interface{}, 20)
}

// AggregatePartitionFile aggregates records from a Query 3 partition file
func (a *Query3Aggregator) AggregatePartitionFile(filePath string, aggregatedData map[string]interface{}) error {
	return a.handler.ReadFileStreaming(filePath, func(record []string) error {
		if len(record) < a.grouper.GetMinFieldCount() {
			log.Printf("Skipping malformed record in %s: %v", filePath, record)
			return nil // Continue processing
		}

		// Process record to get the aggregation key
		key, shouldContinue, err := a.grouper.ProcessRecord(record)
		if err != nil {
			log.Printf("Skipping record in %s: %v", filePath, err)
			return nil // Continue processing
		}
		if !shouldContinue {
			return nil // Skip this record
		}

		// Parse final_amount from the already-aggregated partition file
		// Record format: year,semester,store_id,final_amount (from worker)
		finalAmount, _ := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)

		// Aggregate across partition files
		if aggregatedData[key] == nil {
			aggregatedData[key] = &Query3AggregatedData{}
		}
		agg := aggregatedData[key].(*Query3AggregatedData)
		agg.TotalFinalAmount += finalAmount
		// Count represents number of partition files containing this key
		agg.Count++

		return nil
	})
}
