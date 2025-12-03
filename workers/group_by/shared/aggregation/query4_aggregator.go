package aggregation

import (
	"log"

	"github.com/tp-distribuidos-2c2025/shared/utils"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/grouping"
)

// Query4Aggregator aggregates Query 4 partition files
type Query4Aggregator struct {
	grouper *grouping.Query4Grouper
	handler *utils.CSVHandler
}

// NewQuery4Aggregator creates a new Query 4 aggregator
func NewQuery4Aggregator() (*Query4Aggregator, error) {
	grouper := grouping.NewQuery4Grouper()

	return &Query4Aggregator{
		grouper: grouper,
		handler: utils.NewCSVHandler(""), // baseDir not needed for ReadFileStreaming
	}, nil
}

// GetGrouper returns the grouper
func (a *Query4Aggregator) GetGrouper() grouping.RecordGrouper {
	return a.grouper
}

// InitializeDataMap creates and initializes the aggregated data map for Query 4
func (a *Query4Aggregator) InitializeDataMap() map[string]interface{} {
	return make(map[string]interface{}) // Start empty, let Go grow as needed
}

// AggregatePartitionFile aggregates records from a Query 4 partition file
func (a *Query4Aggregator) AggregatePartitionFile(filePath string, aggregatedData map[string]interface{}) error {
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

		// For Q4, partition files contain user_id,store_id pairs
		// We just count occurrences across all partition files
		if count, ok := aggregatedData[key].(int); ok {
			aggregatedData[key] = count + 1
		} else {
			aggregatedData[key] = 1
		}

		return nil
	})
}
