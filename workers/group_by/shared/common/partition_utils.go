package common

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// CalculateTimeBasedPartition calculates partition based on year and semester
// DEPRECATED: This function is no longer used. All queries now use user-based partitioning.
// Kept for backward compatibility only.
func CalculateTimeBasedPartition(createdAt time.Time) int {
	year := createdAt.Year()
	semester := 1
	if createdAt.Month() >= 7 {
		semester = 2
	}

	switch {
	case year == 2024 && semester == 1:
		return 0
	case year == 2024 && semester == 2:
		return 1
	case year == 2025 && semester == 1:
		return 2
	default:
		// Fallback to partition 0 for unexpected dates
		return 0
	}
}

// CalculateUserBasedPartition calculates partition based on user_id modulo
// Used by Query 4
func CalculateUserBasedPartition(userID string, numPartitions int) (int, error) {
	// Parse user_id as integer (handle both int and float formats)
	userIDFloat, err := strconv.ParseFloat(userID, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid user_id %s: %v", userID, err)
	}
	userIDInt := int(userIDFloat)

	// Calculate partition using modulo
	return userIDInt % numPartitions, nil
}

// CalculateStringHashPartition calculates partition based on string hash
// Used by Query 2 and 3 for transaction_id (UUID) based partitioning
func CalculateStringHashPartition(id string, numPartitions int) (int, error) {
	if id == "" {
		return 0, fmt.Errorf("empty id string")
	}

	// Simple hash: sum of byte values
	hash := 0
	for i := 0; i < len(id); i++ {
		hash = hash*31 + int(id[i])
	}

	// Make hash positive and calculate partition
	if hash < 0 {
		hash = -hash
	}
	return hash % numPartitions, nil
}

// GetPartitionsForWorker returns the list of partitions a worker handles
// All queries use modulo-based distribution (many partitions distributed across workers)
func GetPartitionsForWorker(queryType, workerID, numWorkers, numPartitions int) []int {
	// Worker handles multiple partitions based on modulo
	// Worker i gets partitions where: partition % numWorkers == (workerID % numWorkers)
	// Since workerID starts from 1:
	// Worker 1: partitions 1, 4, 7, 10, ... (partition % 3 == 1)
	// Worker 2: partitions 2, 5, 8, 11, ... (partition % 3 == 2)
	// Worker 3: partitions 0, 3, 6, 9, ... (partition % 3 == 0)
	partitions := []int{}
	targetRemainder := workerID % numWorkers
	for partition := 0; partition < numPartitions; partition++ {
		if partition%numWorkers == targetRemainder {
			partitions = append(partitions, partition)
		}
	}
	return partitions
}

// ExtractPartitionFromFilename extracts partition number from filename
// Filename format: {clientID}-q{queryType}-partition-{XXX}.csv
func ExtractPartitionFromFilename(filePath string) (int, error) {
	fileName := filepath.Base(filePath)
	fileNameWithoutExt := strings.TrimSuffix(fileName, ".csv")

	// Extract partition number (last part after "partition-")
	parts := strings.Split(fileNameWithoutExt, "-partition-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid filename format: %s", fileName)
	}

	partitionStr := parts[1]
	var partition int
	if _, err := fmt.Sscanf(partitionStr, "%d", &partition); err != nil {
		return 0, fmt.Errorf("failed to parse partition number from filename %s: %v", fileName, err)
	}

	return partition, nil
}

// ParseDate parses a date string in various formats
func ParseDate(dateStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006/01/02",
		"2006-01-02T15:04:05Z",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, dateStr); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}
