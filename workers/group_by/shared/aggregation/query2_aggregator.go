package aggregation

import (
	"log"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/shared/utils"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/grouping"
)

// Query2AggregatedData represents aggregated data for Query 2
// Re-exported from grouping package for use in aggregators
type Query2AggregatedData = grouping.Query2AggregatedData

// Query2Aggregator aggregates Query 2 partition files
type Query2Aggregator struct {
	grouper *grouping.Query2Grouper
	handler *utils.CSVHandler
}

// NewQuery2Aggregator creates a new Query 2 aggregator
func NewQuery2Aggregator() (*Query2Aggregator, error) {
	grouper := grouping.NewQuery2Grouper()

	return &Query2Aggregator{
		grouper: grouper,
		handler: utils.NewCSVHandler(""), // baseDir not needed for ReadFileStreaming
	}, nil
}

// GetGrouper returns the grouper
func (a *Query2Aggregator) GetGrouper() grouping.RecordGrouper {
	return a.grouper
}

// InitializeDataMap creates and initializes the aggregated data map for Query 2
func (a *Query2Aggregator) InitializeDataMap() map[string]interface{} {
	return make(map[string]interface{}, 500)
}

// AggregatePartitionFile aggregates records from a Query 2 partition file
func (a *Query2Aggregator) AggregatePartitionFile(filePath string, aggregatedData map[string]interface{}) error {
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

		// Parse quantity and subtotal from the already-aggregated partition file
		// Record format: year,month,item_id,quantity,subtotal (from worker)
		quantity, _ := strconv.Atoi(strings.TrimSpace(record[3]))
		subtotal, _ := strconv.ParseFloat(strings.TrimSpace(record[4]), 64)

		// Aggregate across partition files
		if aggregatedData[key] == nil {
			aggregatedData[key] = &Query2AggregatedData{}
		}
		agg := aggregatedData[key].(*Query2AggregatedData)
		agg.TotalQuantity += quantity
		agg.TotalSubtotal += subtotal
		// Count represents number of partition files containing this key
		agg.Count++

		return nil
	})
}
