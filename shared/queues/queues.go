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
	FixedJoinDataQueue         = "fixed-join-data-queue"
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
	Query2MapQueue           = "query2-map-queue"
	Query2ReduceQueueS2_2023 = "query2-reduce-s2-2023"
	Query2ReduceQueueS1_2024 = "query2-reduce-s1-2024"
	Query2ReduceQueueS2_2024 = "query2-reduce-s2-2024"
	Query2ReduceQueueS1_2025 = "query2-reduce-s1-2025"
	Query2ReduceQueueS2_2025 = "query2-reduce-s2-2025"

	// Group By Orchestrator Queues and Exchanges
	Query2OrchestratorChunksQueue = "query2-orchestrator-chunks"
	Query2MapTerminationExchange  = "query2-map-termination"
	Query3OrchestratorChunksQueue = "query3-orchestrator-chunks"
	Query3MapTerminationExchange  = "query3-map-termination"
	Query4OrchestratorChunksQueue = "query4-orchestrator-chunks"
	Query4MapTerminationExchange  = "query4-map-termination"

	// Query 2 Map-Reduce Exchange and Routing Keys
	Query2MapReduceExchange = "query2-map-reduce"
	Query2RoutingKeyS2_2023 = "query2.semester.2.2023"
	Query2RoutingKeyS1_2024 = "query2.semester.1.2024"
	Query2RoutingKeyS2_2024 = "query2.semester.2.2024"
	Query2RoutingKeyS1_2025 = "query2.semester.1.2025"
	Query2RoutingKeyS2_2025 = "query2.semester.2.2025"

	// Query 3 Map-Reduce Exchange and Routing Keys
	Query3MapReduceExchange = "query3-map-reduce"
	Query3RoutingKeyS2_2023 = "query3.semester.2.2023"
	Query3RoutingKeyS1_2024 = "query3.semester.1.2024"
	Query3RoutingKeyS2_2024 = "query3.semester.2.2024"
	Query3RoutingKeyS1_2025 = "query3.semester.1.2025"
	Query3RoutingKeyS2_2025 = "query3.semester.2.2025"

	// Query 4 Map-Reduce Exchange and Routing Keys
	Query4MapReduceExchange   = "query4-map-reduce"
	Query4RoutingKey          = "query4.all"
	Query2TopItemsQueue       = "query2-top-items-queue"        // Input to top classification
	Query2GroupByResultsQueue = "top-item-classification-chunk" // Output after top classification

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
	Query4TopUsersQueue       = "query4-top-users-queue" // Input to top classification
	Query4GroupByResultsQueue = "userid-join-chunks"     // Output after top classification (to user join)

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

	// ============================================================================
	// In-File Join Orchestrator Queues
	// ============================================================================
	UserPartitionCompletionQueue = "user-partition-completion-queue"
	UserIdCompletionExchange     = "userid-completion-exchange"
	UserIdCompletionRoutingKey   = "userid.completion"

	// ============================================================================
	// In-Memory Join Orchestrator Queues
	// ============================================================================
	InMemoryJoinCompletionQueue = "in-memory-join-completion-queue"
	ItemIdCompletionExchange    = "itemid-completion-exchange"
	ItemIdCompletionRoutingKey  = "itemid.completion"
	StoreIdCompletionExchange   = "storeid-completion-exchange"
	StoreIdCompletionRoutingKey = "storeid.completion"
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

// GetQuery2RoutingKey returns the routing key for a specific semester in Query 2
func GetQuery2RoutingKey(year int, semester int) string {
	if year == 2023 && semester == 2 {
		return Query2RoutingKeyS2_2023
	} else if year == 2024 && semester == 1 {
		return Query2RoutingKeyS1_2024
	} else if year == 2024 && semester == 2 {
		return Query2RoutingKeyS2_2024
	} else if year == 2025 && semester == 1 {
		return Query2RoutingKeyS1_2025
	} else if year == 2025 && semester == 2 {
		return Query2RoutingKeyS2_2025
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

// GetQuery3RoutingKey returns the routing key for a specific semester in Query 3
func GetQuery3RoutingKey(year int, semester int) string {
	if year == 2023 && semester == 2 {
		return Query3RoutingKeyS2_2023
	} else if year == 2024 && semester == 1 {
		return Query3RoutingKeyS1_2024
	} else if year == 2024 && semester == 2 {
		return Query3RoutingKeyS2_2024
	} else if year == 2025 && semester == 1 {
		return Query3RoutingKeyS1_2025
	} else if year == 2025 && semester == 2 {
		return Query3RoutingKeyS2_2025
	}
	return ""
}

// GetQuery4RoutingKey returns the routing key for Query 4 (single routing key)
func GetQuery4RoutingKey() string {
	return Query4RoutingKey
}
