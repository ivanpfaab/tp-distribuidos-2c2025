package joiner

import (
	"strings"

	"github.com/tp-distribuidos-2c2025/workers/join/shared/parser"
)

// Alias types for cleaner API
type MenuItem = parser.MenuItem
type Store = parser.Store

// CSVBuilder builds CSV output for joined data
type CSVBuilder struct {
	builder strings.Builder
	header  string
}

// NewCSVBuilder creates a new CSV builder with header
func NewCSVBuilder(header string) *CSVBuilder {
	cb := &CSVBuilder{
		header: header,
	}
	cb.builder.WriteString(header + "\n")
	return cb
}

// AddRow adds a CSV row
func (cb *CSVBuilder) AddRow(fields ...string) {
	cb.builder.WriteString(strings.Join(fields, ",") + "\n")
}

// String returns the built CSV string
func (cb *CSVBuilder) String() string {
	return cb.builder.String()
}

// BuildTransactionItemMenuJoin builds CSV output for transaction items + menu items join
func BuildTransactionItemMenuJoin(
	transactionItems []map[string]string,
	menuItems map[string]*MenuItem,
) string {
	builder := NewCSVBuilder("transaction_id,item_id,quantity,unit_price,subtotal,created_at,item_name,coffee_category,price,is_seasonal")

	for _, item := range transactionItems {
		itemID := item["item_id"]
		if menuItem, exists := menuItems[itemID]; exists {
			// Join successful - write all fields
			builder.AddRow(
				item["transaction_id"],
				item["item_id"],
				item["quantity"],
				item["unit_price"],
				item["subtotal"],
				item["created_at"],
				menuItem.ItemName,
				menuItem.CoffeeCategory,
				menuItem.Price,
				menuItem.IsSeasonal,
			)
		} else {
			// Join failed - write original data with empty joined fields
			builder.AddRow(
				item["transaction_id"],
				item["item_id"],
				item["quantity"],
				item["unit_price"],
				item["subtotal"],
				item["created_at"],
				"", "", "", "",
			)
		}
	}

	return builder.String()
}

// BuildGroupedTransactionItemMenuJoin builds CSV output for grouped transaction items + menu items join
func BuildGroupedTransactionItemMenuJoin(
	groupedItems []map[string]string,
	menuItems map[string]*MenuItem,
) string {
	builder := NewCSVBuilder("year,month,item_id,quantity,subtotal,category,item_name,coffee_category,price,is_seasonal")

	for _, item := range groupedItems {
		itemID := item["item_id"]
		if menuItem, exists := menuItems[itemID]; exists {
			// Join successful - write all fields
			builder.AddRow(
				item["year"],
				item["month"],
				item["item_id"],
				item["quantity"],
				item["subtotal"],
				item["category"],
				menuItem.ItemName,
				menuItem.CoffeeCategory,
				menuItem.Price,
				menuItem.IsSeasonal,
			)
		} else {
			// Join failed - write original data with empty joined fields
			builder.AddRow(
				item["year"],
				item["month"],
				item["item_id"],
				item["quantity"],
				item["subtotal"],
				item["category"],
				"", "", "", "",
			)
		}
	}

	return builder.String()
}

// BuildTransactionStoreJoin builds CSV output for transactions + stores join
func BuildTransactionStoreJoin(
	transactions []map[string]string,
	stores map[string]*Store,
) string {
	builder := NewCSVBuilder("transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at,store_name,street,postal_code,city,state,latitude,longitude")

	for _, transaction := range transactions {
		storeID := transaction["store_id"]
		if store, exists := stores[storeID]; exists {
			// Join successful - write all fields
			builder.AddRow(
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
				store.StoreName,
				store.Street,
				store.PostalCode,
				store.City,
				store.State,
				store.Latitude,
				store.Longitude,
			)
		} else {
			// Join failed - write original data with empty joined fields
			builder.AddRow(
				transaction["transaction_id"],
				transaction["store_id"],
				transaction["payment_method_id"],
				transaction["voucher_id"],
				transaction["user_id"],
				transaction["original_amount"],
				transaction["discount_applied"],
				transaction["final_amount"],
				transaction["created_at"],
				"", "", "", "", "", "",
			)
		}
	}

	return builder.String()
}

// BuildGroupedTransactionStoreJoin builds CSV output for grouped transactions + stores join
func BuildGroupedTransactionStoreJoin(
	groupedTransactions []map[string]string,
	stores map[string]*Store,
) string {
	builder := NewCSVBuilder("year,semester,store_id,total_final_amount,count,store_name,street,postal_code,city,state,latitude,longitude")

	for _, transaction := range groupedTransactions {
		storeID := transaction["store_id"]
		if store, exists := stores[storeID]; exists {
			// Join successful - write all fields
			builder.AddRow(
				transaction["year"],
				transaction["semester"],
				transaction["store_id"],
				transaction["total_final_amount"],
				transaction["count"],
				store.StoreName,
				store.Street,
				store.PostalCode,
				store.City,
				store.State,
				store.Latitude,
				store.Longitude,
			)
		} else {
			// Join failed - write original data with empty joined fields
			builder.AddRow(
				transaction["year"],
				transaction["semester"],
				transaction["store_id"],
				transaction["total_final_amount"],
				transaction["count"],
				"", "", "", "", "", "",
			)
		}
	}

	return builder.String()
}

