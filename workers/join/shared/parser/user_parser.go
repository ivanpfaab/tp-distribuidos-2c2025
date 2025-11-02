package parser

import (
	"fmt"
)

// TopUserRecord represents a top user from classification
type TopUserRecord struct {
	UserID        string
	StoreID       string
	PurchaseCount string
	Rank          string
}

// UserData represents a user record from CSV file
type UserData struct {
	UserID       string
	Gender       string
	Birthdate    string
	RegisteredAt string
}

// ParseTopUsersData parses top users classification CSV data
func ParseTopUsersData(csvData string) ([]TopUserRecord, error) {
	bp := &BaseParser{}
	records, err := bp.ParseCSV(csvData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	var topUsers []TopUserRecord

	for _, record := range records {
		if bp.IsHeaderRow(record, "user_id") {
			continue
		}
		if !bp.ValidateRecordLength(record, 4) {
			continue
		}

		user := TopUserRecord{
			UserID:        record[0],
			StoreID:       record[1],
			PurchaseCount: record[2],
			Rank:          record[3],
		}
		topUsers = append(topUsers, user)
	}

	return topUsers, nil
}

// ParseUserRecord parses a user record from CSV
func ParseUserRecord(record []string) (map[string]string, error) {
	if len(record) < 4 {
		return nil, fmt.Errorf("invalid user record length: %d", len(record))
	}

	return map[string]string{
		"user_id":       record[0],
		"gender":        record[1],
		"birthdate":      record[2],
		"registered_at": record[3],
	}, nil
}

