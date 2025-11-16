package common

import (
	"fmt"
	"path/filepath"
)

const (
	GroupByDataDir = "/app/groupby-data"
)

// GetBaseDir returns the base directory for a query type and worker
// Format: /app/groupby-data/q{queryType}/worker-{id}/
func GetBaseDir(queryType int, workerID int) string {
	return filepath.Join(GroupByDataDir, fmt.Sprintf("q%d", queryType), fmt.Sprintf("worker-%d", workerID))
}

// GetPartitionFilePath returns the file path for a partition file
// Format: {baseDir}/{clientID}-q{queryType}-partition-{XXX}.csv
func GetPartitionFilePath(baseDir string, clientID string, queryType int, partition int) string {
	filename := fmt.Sprintf("%s-q%d-partition-%03d.csv", clientID, queryType, partition)
	return filepath.Join(baseDir, filename)
}

// GetClientFilePattern returns the file prefix pattern for matching client files
// Format: {clientID}-q{queryType}-partition-
func GetClientFilePattern(clientID string, queryType int) string {
	return fmt.Sprintf("%s-q%d-partition-", clientID, queryType)
}

