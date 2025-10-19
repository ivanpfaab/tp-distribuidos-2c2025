package test

import (
	"testing"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/partitioner"
)

func TestGetSchemaForQueryType(t *testing.T) {
	tests := []struct {
		name           string
		queryType      int
		expectedSchema []string
	}{
		{
			name:      "Query Type 2 - transaction_items schema",
			queryType: 2,
			expectedSchema: []string{
				"transaction_id",
				"item_id",
				"quantity",
				"unit_price",
				"subtotal",
				"created_at",
			},
		},
		{
			name:      "Query Type 3 - transactions schema",
			queryType: 3,
			expectedSchema: []string{
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
		},
		{
			name:      "Query Type 4 - transactions schema",
			queryType: 4,
			expectedSchema: []string{
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
		},
		{
			name:           "Invalid Query Type 1",
			queryType:      1,
			expectedSchema: []string{},
		},
		{
			name:           "Invalid Query Type 5",
			queryType:      5,
			expectedSchema: []string{},
		},
		{
			name:           "Invalid Query Type 0",
			queryType:      0,
			expectedSchema: []string{},
		},
		{
			name:           "Invalid Query Type -1",
			queryType:      -1,
			expectedSchema: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := shared.GetSchemaForQueryType(tt.queryType, shared.RawData)

			if len(schema) != len(tt.expectedSchema) {
				t.Errorf("Expected schema length %d, got %d", len(tt.expectedSchema), len(schema))
				return
			}

			for i, field := range schema {
				if field != tt.expectedSchema[i] {
					t.Errorf("Expected field %d to be %s, got %s", i, tt.expectedSchema[i], field)
				}
			}
		})
	}
}

func TestGetUserPartitionEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		id            string
		numPartitions int
		expectedPart  int
		expectError   bool
	}{
		{
			name:          "ID 0",
			id:            "0",
			numPartitions: 5,
			expectedPart:  0,
			expectError:   false,
		},
		{
			name:          "ID 1",
			id:            "1",
			numPartitions: 5,
			expectedPart:  1,
			expectError:   false,
		},
		{
			name:          "ID equals numPartitions",
			id:            "5",
			numPartitions: 5,
			expectedPart:  0, // 5 % 5 = 0
			expectError:   false,
		},
		{
			name:          "ID greater than numPartitions",
			id:            "7",
			numPartitions: 5,
			expectedPart:  2, // 7 % 5 = 2
			expectError:   false,
		},
		{
			name:          "Large ID",
			id:            "999999",
			numPartitions: 10,
			expectedPart:  9, // 999999 % 10 = 9
			expectError:   false,
		},
		{
			name:          "Float ID",
			id:            "123.0",
			numPartitions: 5,
			expectedPart:  3, // 123 % 5 = 3
			expectError:   false,
		},
		{
			name:          "Float ID with decimal",
			id:            "123.7",
			numPartitions: 5,
			expectedPart:  3, // 123 % 5 = 3 (truncated)
			expectError:   false,
		},
		{
			name:          "Negative ID",
			id:            "-5",
			numPartitions: 5,
			expectedPart:  0, // -5 % 5 = 0
			expectError:   false,
		},
		{
			name:          "Empty string",
			id:            "",
			numPartitions: 5,
			expectError:   true,
		},
		{
			name:          "Non-numeric string",
			id:            "abc",
			numPartitions: 5,
			expectError:   true,
		},
		{
			name:          "Mixed alphanumeric",
			id:            "123abc",
			numPartitions: 5,
			expectError:   true,
		},
		{
			name:          "Only decimal point",
			id:            ".",
			numPartitions: 5,
			expectError:   true,
		},
		{
			name:          "Multiple decimal points",
			id:            "12.3.4",
			numPartitions: 5,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			partition, err := partitioner.GetUserPartition(tt.id, tt.numPartitions)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if partition != tt.expectedPart {
				t.Errorf("Expected partition %d, got %d", tt.expectedPart, partition)
			}

			// Verify partition is within valid range
			if partition < 0 || partition >= tt.numPartitions {
				t.Errorf("Partition %d is out of range [0, %d)", partition, tt.numPartitions)
			}
		})
	}
}

func TestPartitionDistribution(t *testing.T) {
	// Test that partitioning distributes IDs evenly across partitions
	numPartitions := 5
	testIDs := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15"}

	partitionCounts := make(map[int]int)

	for _, id := range testIDs {
		partition, err := partitioner.GetUserPartition(id, numPartitions)
		if err != nil {
			t.Errorf("Unexpected error for ID %s: %v", id, err)
			continue
		}
		partitionCounts[partition]++
	}

	// Check that all partitions received some records
	for i := 0; i < numPartitions; i++ {
		if partitionCounts[i] == 0 {
			t.Errorf("Partition %d received no records", i)
		}
	}

	// Check distribution is roughly even (within 1 record difference)
	expectedPerPartition := len(testIDs) / numPartitions
	for i := 0; i < numPartitions; i++ {
		count := partitionCounts[i]
		if count < expectedPerPartition-1 || count > expectedPerPartition+1 {
			t.Errorf("Partition %d has %d records, expected around %d", i, count, expectedPerPartition)
		}
	}
}
