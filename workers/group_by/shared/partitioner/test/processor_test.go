package test

import (
	"testing"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/partitioner"
)

func TestNewPartitionerProcessor(t *testing.T) {
	tests := []struct {
		name          string
		queryType     int
		numPartitions int
		maxBufferSize int
		expectError   bool
	}{
		{
			name:          "Valid Query Type 2",
			queryType:     2,
			numPartitions: 5,
			maxBufferSize: 100,
			expectError:   false,
		},
		{
			name:          "Valid Query Type 3",
			queryType:     3,
			numPartitions: 10,
			maxBufferSize: 200,
			expectError:   false,
		},
		{
			name:          "Valid Query Type 4",
			queryType:     4,
			numPartitions: 3,
			maxBufferSize: 50,
			expectError:   false,
		},
		{
			name:          "Invalid Query Type",
			queryType:     1,
			numPartitions: 5,
			maxBufferSize: 100,
			expectError:   true,
		},
		{
			name:          "Invalid Query Type 5",
			queryType:     5,
			numPartitions: 5,
			maxBufferSize: 100,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := partitioner.NewPartitionerProcessor(tt.queryType, tt.numPartitions, tt.maxBufferSize)

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

			if processor.QueryType != tt.queryType {
				t.Errorf("Expected queryType %d, got %d", tt.queryType, processor.QueryType)
			}

			if processor.NumPartitions != tt.numPartitions {
				t.Errorf("Expected numPartitions %d, got %d", tt.numPartitions, processor.NumPartitions)
			}

			if processor.MaxBufferSize != tt.maxBufferSize {
				t.Errorf("Expected maxBufferSize %d, got %d", tt.maxBufferSize, processor.MaxBufferSize)
			}

			if len(processor.Partitions) != tt.numPartitions {
				t.Errorf("Expected %d partitions, got %d", tt.numPartitions, len(processor.Partitions))
			}
		})
	}
}

func TestGetUserIDFieldIndex(t *testing.T) {
	tests := []struct {
		name      string
		queryType int
		expected  int
	}{
		{
			name:      "Query Type 2 - item_id",
			queryType: 2,
			expected:  1,
		},
		{
			name:      "Query Type 3 - user_id",
			queryType: 3,
			expected:  4,
		},
		{
			name:      "Query Type 4 - user_id",
			queryType: 4,
			expected:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := partitioner.NewPartitionerProcessor(tt.queryType, 5, 100)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			result := processor.GetUserIDFieldIndex()
			if result != tt.expected {
				t.Errorf("Expected field index %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestGetPartition(t *testing.T) {
	tests := []struct {
		name          string
		queryType     int
		record        partitioner.Record
		numPartitions int
		expectError   bool
	}{
		{
			name:      "Query Type 2 - item_id partitioning",
			queryType: 2,
			record: partitioner.Record{
				Fields: []string{"tx1", "123", "2", "10.5", "21.0", "2024-01-01"},
			},
			numPartitions: 5,
			expectError:   false,
		},
		{
			name:      "Query Type 3 - user_id partitioning",
			queryType: 3,
			record: partitioner.Record{
				Fields: []string{"tx1", "store1", "pm1", "v1", "456", "100.0", "10.0", "90.0", "2024-01-01"},
			},
			numPartitions: 3,
			expectError:   false,
		},
		{
			name:      "Query Type 4 - user_id partitioning",
			queryType: 4,
			record: partitioner.Record{
				Fields: []string{"tx1", "store1", "pm1", "v1", "789", "100.0", "10.0", "90.0", "2024-01-01"},
			},
			numPartitions: 7,
			expectError:   false,
		},
		{
			name:      "Invalid user_id format",
			queryType: 3,
			record: partitioner.Record{
				Fields: []string{"tx1", "store1", "pm1", "v1", "invalid", "100.0", "10.0", "90.0", "2024-01-01"},
			},
			numPartitions: 3,
			expectError:   true,
		},
		{
			name:      "Insufficient fields",
			queryType: 3,
			record: partitioner.Record{
				Fields: []string{"tx1", "store1"},
			},
			numPartitions: 3,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := partitioner.NewPartitionerProcessor(tt.queryType, tt.numPartitions, 100)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			partition, err := partitioner.GetUserPartition(tt.record.Fields[processor.GetUserIDFieldIndex()], tt.numPartitions)

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

			if partition < 0 || partition >= tt.numPartitions {
				t.Errorf("Partition %d is out of range [0, %d)", partition, tt.numPartitions)
			}
		})
	}
}

func TestParseChunkData(t *testing.T) {
	tests := []struct {
		name        string
		queryType   int
		chunkData   string
		expectError bool
		expectedLen int
	}{
		{
			name:      "Valid CSV data with header",
			queryType: 2,
			chunkData: "transaction_id,item_id,quantity,unit_price,subtotal,created_at\n" +
				"tx1,123,2,10.5,21.0,2024-01-01\n" +
				"tx2,456,1,15.0,15.0,2024-01-02",
			expectError: false,
			expectedLen: 2,
		},
		{
			name:        "Empty chunk data",
			queryType:   2,
			chunkData:   "",
			expectError: false,
			expectedLen: 0,
		},
		{
			name:        "Only header row",
			queryType:   3,
			chunkData:   "transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at",
			expectError: false,
			expectedLen: 0,
		},
		{
			name:      "Malformed CSV",
			queryType: 2,
			chunkData: "transaction_id,item_id,quantity,unit_price,subtotal,created_at\n" +
				"tx1,123,2,10.5\n" + // Missing fields
				"tx2,456,1,15.0,15.0,2024-01-02",
			expectError: true, // CSV parsing should fail
			expectedLen: 0,
		},
		{
			name:      "Wrong header schema",
			queryType: 2,
			chunkData: "wrong,header,schema\n" +
				"tx1,123,2",
			expectError: true, // Schema validation should fail
			expectedLen: 0,
		},
		{
			name:      "Wrong field count in header",
			queryType: 2,
			chunkData: "transaction_id,item_id,quantity,unit_price,subtotal\n" + // Missing created_at
				"tx1,123,2,10.5,21.0",
			expectError: true, // Schema validation should fail
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := partitioner.NewPartitionerProcessor(tt.queryType, 5, 100)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			records, err := processor.ParseChunkData(tt.chunkData)

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

			if len(records) != tt.expectedLen {
				t.Errorf("Expected %d records, got %d", tt.expectedLen, len(records))
			}
		})
	}
}

func TestProcessChunk(t *testing.T) {
	processor, err := partitioner.NewPartitionerProcessor(2, 3, 2) // Small buffer size for testing
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	chunkData := "transaction_id,item_id,quantity,unit_price,subtotal,created_at\n" +
		"tx1,123,2,10.5,21.0,2024-01-01\n" +
		"tx2,456,1,15.0,15.0,2024-01-02\n" +
		"tx3,789,3,5.0,15.0,2024-01-03"

	chunk := &chunk.Chunk{
		ClientID:    "client1",
		FileID:      "file1",
		QueryType:   2,
		ChunkNumber: 1,
		IsLastChunk: false,
		Step:        1,
		ChunkSize:   len(chunkData),
		TableID:     1,
		ChunkData:   chunkData,
	}

	err = processor.ProcessChunk(chunk)
	if err != nil {
		t.Errorf("Unexpected error processing chunk: %v", err)
	}

	// Check that records were distributed across partitions
	// Note: With buffer size 2, some records may have been flushed already
	totalRecords := 0
	for i, partition := range processor.Partitions {
		totalRecords += len(partition)
		t.Logf("Partition %d has %d records", i, len(partition))
	}

	// The total should be at most 3 (some may have been flushed due to small buffer size)
	if totalRecords > 3 {
		t.Errorf("Expected at most 3 total records, got %d", totalRecords)
	}
}

func TestFlushPartition(t *testing.T) {
	processor, err := partitioner.NewPartitionerProcessor(2, 3, 100)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	// Add some test records to partition 0
	processor.Partitions[0] = []partitioner.Record{
		{Fields: []string{"tx1", "123", "2", "10.5", "21.0", "2024-01-01"}},
		{Fields: []string{"tx2", "456", "1", "15.0", "15.0", "2024-01-02"}},
	}

	originalChunk := &chunk.Chunk{
		ClientID:    "client1",
		FileID:      "file1",
		QueryType:   2,
		ChunkNumber: 1,
		IsLastChunk: false,
		Step:        1,
		ChunkSize:   100,
		TableID:     1,
		ChunkData:   "test data",
	}

	// Test flushing partition
	err = processor.FlushPartition(0, originalChunk, false)
	if err != nil {
		t.Errorf("Unexpected error flushing partition: %v", err)
	}

	// Check that partition was cleared
	if len(processor.Partitions[0]) != 0 {
		t.Errorf("Expected partition to be empty after flush, got %d records", len(processor.Partitions[0]))
	}
}

func TestPartitionToCSV(t *testing.T) {
	processor, err := partitioner.NewPartitionerProcessor(2, 3, 100)
	if err != nil {
		t.Fatalf("Failed to create processor: %v", err)
	}

	// Add test records to partition 0
	processor.Partitions[0] = []partitioner.Record{
		{Fields: []string{"tx1", "123", "2", "10.5", "21.0", "2024-01-01"}},
		{Fields: []string{"tx2", "456", "1", "15.0", "15.0", "2024-01-02"}},
	}

	csvData := processor.PartitionToCSV(0)

	expectedHeader := "transaction_id,item_id,quantity,unit_price,subtotal,created_at"
	if !contains(csvData, expectedHeader) {
		t.Errorf("CSV data should contain header: %s", expectedHeader)
	}

	expectedRecord1 := "tx1,123,2,10.5,21.0,2024-01-01"
	if !contains(csvData, expectedRecord1) {
		t.Errorf("CSV data should contain record: %s", expectedRecord1)
	}

	expectedRecord2 := "tx2,456,1,15.0,15.0,2024-01-02"
	if !contains(csvData, expectedRecord2) {
		t.Errorf("CSV data should contain record: %s", expectedRecord2)
	}
}

func TestGetUserPartition(t *testing.T) {
	tests := []struct {
		name          string
		id            string
		numPartitions int
		expectError   bool
	}{
		{
			name:          "Valid integer ID",
			id:            "123",
			numPartitions: 5,
			expectError:   false,
		},
		{
			name:          "Valid float ID",
			id:            "123.0",
			numPartitions: 5,
			expectError:   false,
		},
		{
			name:          "Large ID",
			id:            "999999",
			numPartitions: 10,
			expectError:   false,
		},
		{
			name:          "Invalid ID format",
			id:            "invalid",
			numPartitions: 5,
			expectError:   true,
		},
		{
			name:          "Empty ID",
			id:            "",
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

			if partition < 0 || partition >= tt.numPartitions {
				t.Errorf("Partition %d is out of range [0, %d)", partition, tt.numPartitions)
			}
		})
	}
}

func TestValidateHeader(t *testing.T) {
	tests := []struct {
		name        string
		queryType   int
		header      []string
		expectError bool
	}{
		{
			name:        "Valid Query Type 2 header",
			queryType:   2,
			header:      []string{"transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at"},
			expectError: false,
		},
		{
			name:        "Valid Query Type 3 header",
			queryType:   3,
			header:      []string{"transaction_id", "store_id", "payment_method_id", "voucher_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at"},
			expectError: false,
		},
		{
			name:        "Valid Query Type 4 header",
			queryType:   4,
			header:      []string{"transaction_id", "store_id", "payment_method_id", "voucher_id", "user_id", "original_amount", "discount_applied", "final_amount", "created_at"},
			expectError: false,
		},
		{
			name:        "Wrong field count",
			queryType:   2,
			header:      []string{"transaction_id", "item_id", "quantity", "unit_price", "subtotal"}, // Missing created_at
			expectError: true,
		},
		{
			name:        "Wrong field names",
			queryType:   2,
			header:      []string{"wrong", "header", "schema", "with", "wrong", "fields"},
			expectError: true,
		},
		{
			name:        "Empty header",
			queryType:   2,
			header:      []string{},
			expectError: true,
		},
		{
			name:        "Header with extra fields",
			queryType:   2,
			header:      []string{"transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at", "extra_field"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, err := partitioner.NewPartitionerProcessor(tt.queryType, 5, 100)
			if err != nil {
				t.Fatalf("Failed to create processor: %v", err)
			}

			err = processor.ValidateHeader(tt.header)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
