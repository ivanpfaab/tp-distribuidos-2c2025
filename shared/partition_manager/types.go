package partitionmanager

// PartitionData represents data to be written to a partition
type PartitionData struct {
	Number int      // Partition number (0 to numPartitions-1)
	Lines  []string // CSV records as []string (already parsed, each line should end with \n)
}

// WriteOptions configures write behavior
type WriteOptions struct {
	FilePrefix string   // e.g., "users-partition" -> "users-partition-000.csv"
	Header     []string // CSV header (written if file doesn't exist)
	ClientID   string   // Client identifier for file naming
}

