package aggregation

import (
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/grouping"
)

// Aggregator handles aggregation of partition files
type Aggregator interface {
	// AggregatePartitionFile aggregates records from a single partition file
	// Returns the aggregated data map (key -> aggregated value)
	AggregatePartitionFile(filePath string, aggregatedData map[string]interface{}) error
	// GetGrouper returns the grouper used by this aggregator
	GetGrouper() grouping.RecordGrouper
	// InitializeDataMap creates and initializes the aggregated data map
	InitializeDataMap() map[string]interface{}
}

