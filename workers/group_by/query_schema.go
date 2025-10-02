package main

// DataSchema represents the type of data being processed
type DataSchema int

const (
	// RawData represents the original input data schema
	RawData DataSchema = iota
	// GroupedData represents the already-grouped data schema from workers
	GroupedData
)

// QueryType2Schemas defines the schemas for QueryType 2 (transaction_items - group by year, month, item_id)
type QueryType2Schemas struct {
	RawSchema     []string
	GroupedSchema []string
}

// QueryType3Schemas defines the schemas for QueryType 3 (transactions - group by year, semester, store_id)
type QueryType3Schemas struct {
	RawSchema     []string
	GroupedSchema []string
}

// QueryType4Schemas defines the schemas for QueryType 4 (transactions - group by user_id, store_id)
type QueryType4Schemas struct {
	RawSchema     []string
	GroupedSchema []string
}

// GetQueryType2Schemas returns the schemas for QueryType 2
func GetQueryType2Schemas() QueryType2Schemas {
	return QueryType2Schemas{
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

// GetQueryType3Schemas returns the schemas for QueryType 3
func GetQueryType3Schemas() QueryType3Schemas {
	return QueryType3Schemas{
		RawSchema: []string{
			"transaction_id",
			"store_id",
			"payment_method_id",
			"voucher_id",
			"user_id",
			"original_amount",
			"discount_applied",
			"final_amount",
			"created_at",
		},
		GroupedSchema: []string{
			"year",
			"semester",
			"store_id",
			"total_final_amount",
			"count",
		},
	}
}

// GetQueryType4Schemas returns the schemas for QueryType 4
func GetQueryType4Schemas() QueryType4Schemas {
	return QueryType4Schemas{
		RawSchema: []string{
			"transaction_id",
			"store_id",
			"payment_method_id",
			"voucher_id",
			"user_id",
			"original_amount",
			"discount_applied",
			"final_amount",
			"created_at",
		},
		GroupedSchema: []string{
			"user_id",
			"store_id",
			"count",
		},
	}
}

// GetSchemaForQueryType returns the appropriate schema for a given query type and data type
func GetSchemaForQueryType(queryType int, dataType DataSchema) []string {
	switch queryType {
	case 1:
		// QueryType 1 has no group by - return empty schema
		return []string{}
	case 2:
		schemas := GetQueryType2Schemas()
		if dataType == RawData {
			return schemas.RawSchema
		}
		return schemas.GroupedSchema
	case 3:
		schemas := GetQueryType3Schemas()
		if dataType == RawData {
			return schemas.RawSchema
		}
		return schemas.GroupedSchema
	case 4:
		schemas := GetQueryType4Schemas()
		if dataType == RawData {
			return schemas.RawSchema
		}
		return schemas.GroupedSchema
	default:
		return []string{}
	}
}
