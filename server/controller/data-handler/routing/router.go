package routing

import "strings"

// FileType represents the type of file being processed
type FileType int

const (
	FileTypeReference   FileType = iota // Reference data (stores, menu_items, users)
	FileTypeTransaction                 // Transaction data (transactions, transaction_items)
	FileTypeUnknown                     // Unknown file type
)

// FileTypeRouter routes files based on their file ID
type FileTypeRouter struct{}

// NewFileTypeRouter creates a new file type router
func NewFileTypeRouter() *FileTypeRouter {
	return &FileTypeRouter{}
}

// ClassifyFile classifies a file based on its file ID
func (r *FileTypeRouter) ClassifyFile(fileID string) FileType {
	if r.IsReferenceData(fileID) {
		return FileTypeReference
	}
	if r.IsTransactionData(fileID) {
		return FileTypeTransaction
	}
	return FileTypeUnknown
}

// IsReferenceData checks if the file ID belongs to reference data files
func (r *FileTypeRouter) IsReferenceData(fileID string) bool {
	// Reference data files that need to be stored in writer for joins:
	// - ST: stores.csv (for store_id joins in query 3)
	// - MN: menu_items.csv (for item_id joins in query 2)
	// - US: users_*.csv (for user_id joins in query 4)
	return strings.HasPrefix(fileID, "ST") || strings.HasPrefix(fileID, "MN") || strings.HasPrefix(fileID, "US")
}

// IsTransactionData checks if the file ID belongs to transaction data files
func (r *FileTypeRouter) IsTransactionData(fileID string) bool {
	// Transaction data files:
	// - TR: transactions_*.csv (for store_id and user_id joins)
	// - TI: transaction_items_*.csv (for item_id joins)
	return strings.HasPrefix(fileID, "TR") || strings.HasPrefix(fileID, "TI")
}

// GetQueryType determines the query type based on the file ID
func (r *FileTypeRouter) GetQueryType(fileID string) uint8 {
	// Query type mapping based on file type:
	// Query 2: transaction_items ↔ menu_items (on item_id)
	// Query 3: transactions ↔ stores (on store_id)
	// Query 4: transactions ↔ users (on user_id)

	if strings.HasPrefix(fileID, "TI") {
		// Transaction items files - Query 2 (item_id joins with menu_items)
		return 2
	}
	if strings.HasPrefix(fileID, "TR") {
		// Transaction files - Query 3 (store_id joins with stores)
		// Note: For Query 4, the same transaction files are used but with different routing
		return 3
	}
	// Default for reference data (determined by file type)
	return 3
}

// GetQueryTypesForTransactionFile returns all query types that use a transaction file
func (r *FileTypeRouter) GetQueryTypesForTransactionFile(fileID string) []uint8 {
	if strings.HasPrefix(fileID, "TR") {
		// Transaction files are used by Query 1, 3, and 4
		return []uint8{1, 3, 4}
	}
	if strings.HasPrefix(fileID, "TI") {
		// Transaction items files are used by Query 2
		return []uint8{2}
	}
	return []uint8{}
}
