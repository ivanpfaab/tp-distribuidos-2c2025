package parser

import (
	"fmt"
	"strings"
)

// MenuItem represents a menu item record
type MenuItem struct {
	ItemID        string
	ItemName      string
	Category      string
	Price         string
	IsSeasonal    string
	AvailableFrom string
	AvailableTo   string
}

// Store represents a store record
type Store struct {
	StoreID    string
	StoreName  string
	Street     string
	PostalCode string
	City       string
	State      string
	Latitude   string
	Longitude  string
}

// ParseMenuItems parses menu items CSV data
func ParseMenuItems(csvData string, clientID string) (map[string]*MenuItem, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, err
	}

	menuItems := make(map[string]*MenuItem)

	for _, record := range records {
		// Skip header
		if bp.IsHeaderRow(record, "item_id") {
			continue
		}

		if !bp.ValidateRecordLength(record, 7) {
			continue
		}

		menuItem := &MenuItem{
			ItemID:        record[0],
			ItemName:      record[1],
			Category:      record[2],
			Price:         record[3],
			IsSeasonal:    record[4],
			AvailableFrom: record[5],
			AvailableTo:   record[6],
		}
		menuItems[menuItem.ItemID] = menuItem
	}

	return menuItems, nil
}

// ParseStores parses stores CSV data
func ParseStores(csvData string, clientID string) (map[string]*Store, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, err
	}

	stores := make(map[string]*Store)

	for _, record := range records {
		// Skip header
		if bp.IsHeaderRow(record, "store_id") {
			continue
		}

		if !bp.ValidateRecordLength(record, 8) {
			continue
		}

		store := &Store{
			StoreID:    record[0],
			StoreName:  record[1],
			Street:     record[2],
			PostalCode: record[3],
			City:       record[4],
			State:      record[5],
			Latitude:   record[6],
			Longitude:  record[7],
		}
		stores[store.StoreID] = store
	}

	return stores, nil
}

// ParseTransactionItems parses transaction items CSV data
func ParseTransactionItems(csvData string) ([]map[string]string, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var transactionItems []map[string]string

	for _, record := range records {
		if bp.IsHeaderRow(record, "transaction_id") {
			continue
		}
		if !bp.ValidateRecordLength(record, 6) {
			continue
		}

		item := map[string]string{
			"transaction_id": record[0],
			"item_id":        record[1],
			"quantity":       record[2],
			"unit_price":     record[3],
			"subtotal":       record[4],
			"created_at":     record[5],
		}
		transactionItems = append(transactionItems, item)
	}

	return transactionItems, nil
}

// ParseTransactions parses transactions CSV data
func ParseTransactions(csvData string) ([]map[string]string, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var transactions []map[string]string

	for _, record := range records {
		if bp.IsHeaderRow(record, "transaction_id") {
			continue
		}
		if !bp.ValidateRecordLength(record, 9) {
			continue
		}

		transaction := map[string]string{
			"transaction_id":    record[0],
			"store_id":          record[1],
			"payment_method_id": record[2],
			"voucher_id":        record[3],
			"user_id":           record[4],
			"original_amount":   record[5],
			"discount_applied":  record[6],
			"final_amount":      record[7],
			"created_at":        record[8],
		}
		transactions = append(transactions, transaction)
	}

	return transactions, nil
}

// IsGroupedData checks if the data is grouped data from GroupBy
func IsGroupedData(data string, expectedFields ...string) bool {
	lines := strings.Split(data, "\n")
	if len(lines) < 1 {
		return false
	}

	header := strings.ToLower(lines[0])
	for _, field := range expectedFields {
		if !strings.Contains(header, strings.ToLower(field)) {
			return false
		}
	}
	return true
}

// ParseGroupedTransactionItems parses grouped transaction items data from GroupBy
func ParseGroupedTransactionItems(csvData string) ([]map[string]string, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var groupedItems []map[string]string

	for _, record := range records {
		if bp.IsHeaderRow(record, "year") {
			continue
		}
		if !bp.ValidateRecordLength(record, 6) {
			continue
		}

		item := map[string]string{
			"year":     record[0],
			"month":    record[1],
			"item_id":  record[2],
			"quantity": record[3],
			"subtotal": record[4],
			"count":    record[5],
		}
		groupedItems = append(groupedItems, item)
	}

	return groupedItems, nil
}

// ParseGroupedTransactions parses grouped transaction data from GroupBy (Query Type 3)
func ParseGroupedTransactions(csvData string) ([]map[string]string, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var groupedTransactions []map[string]string

	for _, record := range records {
		if bp.IsHeaderRow(record, "year") {
			continue
		}
		if !bp.ValidateRecordLength(record, 5) {
			continue
		}

		transaction := map[string]string{
			"year":               record[0],
			"semester":           record[1],
			"store_id":           record[2],
			"total_final_amount": record[3],
			"count":              record[4],
		}
		groupedTransactions = append(groupedTransactions, transaction)
	}

	return groupedTransactions, nil
}

