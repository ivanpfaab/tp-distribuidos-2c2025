package shared

// Record types for different query types used in groupby operations
// These types are used across worker, processor, and file manager components

// Query2Record represents a month, item_id, quantity, subtotal record for Query 2
type Query2Record struct {
	Month    string
	ItemID   string
	Quantity string
	Subtotal string
}

// Query3Record represents a store_id, final_amount record for Query 3
type Query3Record struct {
	StoreID     string
	FinalAmount string
}

// Query4Record represents a user_id, store_id pair for Query 4
type Query4Record struct {
	UserID    string
	StoreID   string
	Partition int // Optional: partition number for routing
}

// ToCSVRow converts Query2Record to CSV row
func (r Query2Record) ToCSVRow() []string {
	return []string{r.Month, r.ItemID, r.Quantity, r.Subtotal}
}

// ToCSVRow converts Query3Record to CSV row
func (r Query3Record) ToCSVRow() []string {
	return []string{r.StoreID, r.FinalAmount}
}

// ToCSVRow converts Query4Record to CSV row
func (r Query4Record) ToCSVRow() []string {
	return []string{r.UserID, r.StoreID}
}
