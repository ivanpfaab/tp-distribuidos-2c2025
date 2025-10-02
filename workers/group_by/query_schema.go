package main

// DataSchema represents the type of data being processed
type DataSchema int

const (
	// RawData represents the original input data schema
	RawData DataSchema = iota
	// GroupedData represents the already-grouped data schema from workers
	GroupedData
)

// QueryType1Schemas defines the schemas for QueryType 1 (transaction_items)
type QueryType1Schemas struct {
	RawSchema     []string
	GroupedSchema []string
}

// GetQueryType1Schemas returns the schemas for QueryType 1
func GetQueryType1Schemas() QueryType1Schemas {
	return QueryType1Schemas{
		RawSchema: []string{
			"transaction_id",
			"item_id",
			"quantity",
			"unit_price",
			"subtotal",
			"created_at",
		},
		GroupedSchema: []string{
			"year",
			"month",
			"item_id",
			"quantity", // total_quantity
			"subtotal", // total_subtotal
			"count",    // record_count
		},
	}
}

// GetSchemaForQueryType returns the appropriate schema for a given query type and data type
func GetSchemaForQueryType(queryType int, dataType DataSchema) []string {
	switch queryType {
	case 1:
		schemas := GetQueryType1Schemas()
		if dataType == RawData {
			return schemas.RawSchema
		}
		return schemas.GroupedSchema
	default:
		return []string{}
	}
}

// IsGroupedData checks if the data appears to be grouped based on its schema
func IsGroupedData(data string, queryType int) bool {
	if queryType != 1 {
		return false
	}

	// Check if the first line contains grouped schema indicators
	groupedSchema := GetQueryType1Schemas().GroupedSchema
	firstLine := data
	if len(data) > 0 {
		// Get first line
		for i, char := range data {
			if char == '\n' {
				firstLine = data[:i]
				break
			}
		}
	}

	// Check if it contains grouped schema fields
	for _, field := range groupedSchema {
		if !contains(firstLine, field) {
			return false
		}
	}
	return true
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				containsMiddle(s, substr))))
}

// containsMiddle checks if substr is contained in the middle of s
func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
