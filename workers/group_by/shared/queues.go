package shared

import "fmt"

const (
	// Input queues for Map workers
	Query2MapQueue = "query2-map-queue" // Query 2 Map Worker input
	Query3MapQueue = "query3-map-queue" // Query 3 Map Worker input
	Query4MapQueue = "query4-map-queue" // Query 4 Map Worker input

	// Legacy/alternate names (for backwards compatibility)
	ItemIdGroupByChunkQueue  = Query2MapQueue // Alias for Query 2
	StoreIdGroupByChunkQueue = Query3MapQueue // Alias for Query 3

	// Output queues for Reduce workers final results
	Query2GroupByResultsQueue = "query2-groupby-results-queue" // Query 2 Reduce Workers output
	Query3GroupByResultsQueue = "query3-groupby-results-queue" // Query 3 Reduce Workers output
	Query4GroupByResultsQueue = "query4-groupby-results-queue" // Query 4 Reduce Workers output

	// Query 4 reduce queue (map -> reduce)
	Query4ReduceQueue = "query4-reduce-queue"
)

// GetQuery2ReduceQueueName returns the reduce queue name for Query 2 for a specific semester
// Format: "query2-reduce-{year}-{semester}"
func GetQuery2ReduceQueueName(year, semester int) string {
	return fmt.Sprintf("query2-reduce-%d-%d", year, semester)
}

// GetQuery3ReduceQueueName returns the reduce queue name for Query 3 for a specific semester
// Format: "query3-reduce-{year}-{semester}"
func GetQuery3ReduceQueueName(year, semester int) string {
	return fmt.Sprintf("query3-reduce-%d-%d", year, semester)
}
