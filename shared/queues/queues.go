package queues

// This package centralizes all queue names used across the distributed system
// to ensure consistency and avoid duplication

const (
	// ============================================================================
	// Filter Worker Queues
	// ============================================================================
	YearFilterQueue   = "year-filter-queue"
	TimeFilterQueue   = "time-filter-queue"
	AmountFilterQueue = "amount-filter-queue"

	// ============================================================================
	// Join Worker Queues
	// ============================================================================
	// Join Data Handler
	FixedJoinDataExchange      = "fixed-join-data-exchange"
	FixedJoinDataRoutingKey    = "fixed-join-data"
	JoinItemIdDictionaryQueue  = "join-itemid-dictionary"
	JoinStoreIdDictionaryQueue = "join-storeid-dictionary"
	JoinUserIdDictionaryQueue  = "join-userid-dictionary"

	// In-Memory Join Workers - Input Queues
	ItemIdDictionaryQueue  = "join-itemid-dictionary"
	ItemIdChunkQueue       = "top-item-classification-chunk"
	StoreIdDictionaryQueue = "join-storeid-dictionary"
	StoreIdChunkQueue      = "itemid-join-chunks"

	// In-File Join Worker (Query 4) - Input Queues
	UserIdChunkQueue = "userid-join-chunks"

	// Join Results Queues
	Query2ResultsQueue = "query2-results-chunks"
	Query3ResultsQueue = "query3-results-chunks"

	StoreIdDictionaryExchange   = "storeid-dictionary-exchange"
	StoreIdDictionaryRoutingKey = "storeid-dictionary"

	ItemIdDictionaryExchange   = "itemid-dictionary-exchange"
	ItemIdDictionaryRoutingKey = "itemid-dictionary"

	// ============================================================================
	// Group By MapReduce Queues
	// ============================================================================
	// Query 2 (transaction_items - group by year, month, item_id)
	Query2MapQueue            = "query2-map-queue"
	Query2ReduceQueueS2_2023  = "query2-reduce-s2-2023"
	Query2ReduceQueueS1_2024  = "query2-reduce-s1-2024"
	Query2ReduceQueueS2_2024  = "query2-reduce-s2-2024"
	Query2ReduceQueueS1_2025  = "query2-reduce-s1-2025"
	Query2ReduceQueueS2_2025  = "query2-reduce-s2-2025"
	Query2GroupByResultsQueue = "top-items-queue"

	// Group By Orchestrator Queues and Exchanges
	Query2OrchestratorChunksQueue = "query2-orchestrator-chunks"
	Query2MapTerminationExchange  = "query2-map-termination"

	// Query 3 (transactions - group by year, semester, store_id)
	Query3MapQueue            = "query3-map-queue"
	Query3ReduceQueueS2_2023  = "query3-reduce-s2-2023"
	Query3ReduceQueueS1_2024  = "query3-reduce-s1-2024"
	Query3ReduceQueueS2_2024  = "query3-reduce-s2-2024"
	Query3ReduceQueueS1_2025  = "query3-reduce-s1-2025"
	Query3ReduceQueueS2_2025  = "query3-reduce-s2-2025"
	Query3GroupByResultsQueue = "itemid-join-chunks"

	// Query 4 (transactions - group by user_id, store_id)
	Query4MapQueue            = "query4-map-queue"
	Query4ReduceQueue         = "query4-reduce-queue"
	Query4GroupByResultsQueue = "top-users-queue"

	// ============================================================================
	// Query Gateway Queues
	// ============================================================================
	ReplyFilterBusQueue = "reply-filter-bus"

	// ============================================================================
	// Results Queues
	// ============================================================================
	Query1ResultsQueue = "query1-results-chunks"
	Query4ResultsQueue = "query4-results-chunks"

	// ============================================================================
	// Streaming Service Queues
	// ============================================================================
	StreamingServiceQueue = "streaming-service-queue"
)

// GetQuery2ReduceQueueName returns the reduce queue name for a specific semester in Query 2
func GetQuery2ReduceQueueName(year int, semester int) string {
	if year == 2023 && semester == 2 {
		return Query2ReduceQueueS2_2023
	} else if year == 2024 && semester == 1 {
		return Query2ReduceQueueS1_2024
	} else if year == 2024 && semester == 2 {
		return Query2ReduceQueueS2_2024
	} else if year == 2025 && semester == 1 {
		return Query2ReduceQueueS1_2025
	} else if year == 2025 && semester == 2 {
		return Query2ReduceQueueS2_2025
	}
	return ""
}

// GetQuery3ReduceQueueName returns the reduce queue name for a specific semester in Query 3
func GetQuery3ReduceQueueName(year int, semester int) string {
	if year == 2023 && semester == 2 {
		return Query3ReduceQueueS2_2023
	} else if year == 2024 && semester == 1 {
		return Query3ReduceQueueS1_2024
	} else if year == 2024 && semester == 2 {
		return Query3ReduceQueueS2_2024
	} else if year == 2025 && semester == 1 {
		return Query3ReduceQueueS1_2025
	} else if year == 2025 && semester == 2 {
		return Query3ReduceQueueS2_2025
	}
	return ""
}
