package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// Query2RecordExtractor extracts Query2Record from CSV rows
type Query2RecordExtractor struct{}

func (e *Query2RecordExtractor) GetMinFieldCount() int {
	return 6 // transaction_id, item_id, quantity, unit_price, subtotal, created_at
}

func (e *Query2RecordExtractor) GetQueryName() string {
	return "Query 2"
}

func (e *Query2RecordExtractor) ExtractRecord(record []string, workerPartitions map[int]bool, numPartitions int) (int, interface{}, error) {
	// Parse fields
	transactionID := strings.TrimSpace(record[0])
	itemID := strings.TrimSpace(record[1])
	quantity := strings.TrimSpace(record[2])
	subtotal := strings.TrimSpace(record[4])

	// Validate fields are not empty
	if transactionID == "" || quantity == "" || subtotal == "" {
		return 0, nil, fmt.Errorf("empty transaction_id, quantity, or subtotal")
	}

	// Parse created_at to extract year and month
	createdAt, err := common.ParseDate(strings.TrimSpace(record[5]))
	if err != nil {
		return 0, nil, fmt.Errorf("invalid date: %v", err)
	}

	year := fmt.Sprintf("%d", createdAt.Year())
	month := fmt.Sprintf("%02d", createdAt.Month()) // Zero-padded to match partitioner

	// Calculate partition using composite key (year, month, itemID)
	// MUST match the partitioner's logic for consistent routing
	partition, err := common.CalculateCompositeHashPartition([]string{year, month, itemID}, numPartitions)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to calculate partition: %v", err)
	}

	// Only process records belonging to partitions this worker owns
	if !workerPartitions[partition] {
		return 0, nil, fmt.Errorf("partition %d not owned by worker", partition)
	}

	// Create record
	rec := shared.Query2Record{
		Year:     year,
		Month:    month,
		ItemID:   itemID,
		Quantity: quantity,
		Subtotal: subtotal,
	}

	return partition, rec, nil
}

// Query3RecordExtractor extracts Query3Record from CSV rows
type Query3RecordExtractor struct{}

func (e *Query3RecordExtractor) GetMinFieldCount() int {
	return 9 // transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at
}

func (e *Query3RecordExtractor) GetQueryName() string {
	return "Query 3"
}

func (e *Query3RecordExtractor) ExtractRecord(record []string, workerPartitions map[int]bool, numPartitions int) (int, interface{}, error) {
	// Parse fields
	transactionID := strings.TrimSpace(record[0])
	storeID := strings.TrimSpace(record[1])
	finalAmount := strings.TrimSpace(record[7])

	// Validate fields are not empty
	if transactionID == "" || storeID == "" || finalAmount == "" {
		return 0, nil, fmt.Errorf("empty transaction_id, store_id, or final_amount")
	}

	// Parse created_at to extract year and semester
	createdAt, err := common.ParseDate(strings.TrimSpace(record[8]))
	if err != nil {
		return 0, nil, fmt.Errorf("invalid date: %v", err)
	}

	year := fmt.Sprintf("%d", createdAt.Year())

	// Calculate semester using the EXACT same logic as the partitioner
	monthInt := int(createdAt.Month())
	semester := "1"
	if monthInt >= 7 && monthInt <= 12 {
		semester = "2"
	}

	// Calculate partition using composite key (year, semester, storeID)
	// MUST match the partitioner's logic for consistent routing
	partition, err := common.CalculateCompositeHashPartition([]string{year, semester, storeID}, numPartitions)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to calculate partition: %v", err)
	}

	// Only process records belonging to partitions this worker owns
	if !workerPartitions[partition] {
		return 0, nil, fmt.Errorf("partition %d not owned by worker", partition)
	}

	// Create record
	rec := shared.Query3Record{
		Year:        year,
		Semester:    semester,
		StoreID:     storeID,
		FinalAmount: finalAmount,
	}

	return partition, rec, nil
}

// Query4RecordExtractor extracts Query4Record from CSV rows
type Query4RecordExtractor struct{}

func (e *Query4RecordExtractor) GetMinFieldCount() int {
	return 9 // transaction_id, store_id, payment_method_id, voucher_id, user_id, original_amount, discount_applied, final_amount, created_at
}

func (e *Query4RecordExtractor) GetQueryName() string {
	return "Query 4"
}

func (e *Query4RecordExtractor) ExtractRecord(record []string, workerPartitions map[int]bool, numPartitions int) (int, interface{}, error) {
	// Parse fields
	storeID := strings.TrimSpace(record[1])
	userID := strings.TrimSpace(record[4])

	// Skip if user_id is empty
	if userID == "" {
		return 0, nil, fmt.Errorf("empty user_id")
	}

	// Calculate partition for this record using common utility
	recordPartition, err := common.CalculateUserBasedPartition(userID, numPartitions)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid user_id %s: %v", userID, err)
	}

	// Only process records belonging to partitions this worker owns
	if !workerPartitions[recordPartition] {
		return 0, nil, fmt.Errorf("partition %d not owned by worker", recordPartition)
	}

	// Create record
	rec := shared.Query4Record{
		UserID:    userID,
		StoreID:   storeID,
		Partition: recordPartition,
	}

	return recordPartition, rec, nil
}
