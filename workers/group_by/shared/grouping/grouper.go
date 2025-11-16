package grouping

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

