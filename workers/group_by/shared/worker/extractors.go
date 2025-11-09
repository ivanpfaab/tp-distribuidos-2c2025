package main

import (
	"fmt"
	"strings"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
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
	itemID := strings.TrimSpace(record[1])
	quantity := strings.TrimSpace(record[2])
	subtotal := strings.TrimSpace(record[4])

	// Validate quantity and subtotal are not empty
	if quantity == "" || subtotal == "" {
		return 0, nil, fmt.Errorf("empty quantity or subtotal")
	}

	// Parse created_at to extract month and calculate partition
	createdAt, err := shared.ParseDate(strings.TrimSpace(record[5]))
	if err != nil {
		return 0, nil, fmt.Errorf("invalid date: %v", err)
	}

	month := fmt.Sprintf("%d", createdAt.Month())

	// Calculate partition using shared utility
	partition := shared.CalculateTimeBasedPartition(createdAt)

	// Only process records belonging to partitions this worker owns
	if !workerPartitions[partition] {
		return 0, nil, fmt.Errorf("partition %d not owned by worker", partition)
	}

	// Create record
	rec := shared.Query2Record{
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
	storeID := strings.TrimSpace(record[1])
	finalAmount := strings.TrimSpace(record[7])

	// Validate fields are not empty
	if storeID == "" || finalAmount == "" {
		return 0, nil, fmt.Errorf("empty store_id or final_amount")
	}

	// Parse created_at and calculate partition using shared utility
	createdAt, err := shared.ParseDate(strings.TrimSpace(record[8]))
	if err != nil {
		return 0, nil, fmt.Errorf("invalid date: %v", err)
	}

	// Calculate partition using shared utility
	partition := shared.CalculateTimeBasedPartition(createdAt)

	// Only process records belonging to partitions this worker owns
	if !workerPartitions[partition] {
		return 0, nil, fmt.Errorf("partition %d not owned by worker", partition)
	}

	// Create record
	rec := shared.Query3Record{
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

	// Calculate partition for this record using shared utility
	recordPartition, err := shared.CalculateUserBasedPartition(userID, numPartitions)
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

