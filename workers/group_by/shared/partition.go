package shared

import (
	"fmt"
	"strconv"
	"time"
)

// PartitionCalculator provides partition calculation logic for different query types
type PartitionCalculator struct {
	queryType     int
	numPartitions int
}

// NewPartitionCalculator creates a new partition calculator for a query type
func NewPartitionCalculator(queryType, numPartitions int) *PartitionCalculator {
	return &PartitionCalculator{
		queryType:     queryType,
		numPartitions: numPartitions,
	}
}

// CalculateTimeBasedPartition calculates partition based on year and semester
// Used by Query 2 and Query 3
// Partition 0 = 2024-S1, Partition 1 = 2024-S2, Partition 2 = 2025-S1
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

// GetPartitionsForWorker returns the list of partitions a worker handles
// For Query 2 & 3: 1:1 mapping (Worker 1 → Partition 0, Worker 2 → Partition 1, Worker 3 → Partition 2)
// For Query 4: Modulo-based distribution (many partitions distributed across workers)
func GetPartitionsForWorker(queryType, workerID, numWorkers, numPartitions int) []int {
	// For Q2/Q3: worker ID maps to partition (1:1 mapping)
	// Worker IDs are 1-based (1, 2, 3), partitions are 0-based (0, 1, 2)
	if queryType == 2 || queryType == 3 {
		return []int{workerID - 1}
	}

	// For Q4: worker handles multiple partitions based on modulo
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
