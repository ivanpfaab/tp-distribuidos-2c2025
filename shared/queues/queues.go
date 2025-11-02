package queues

import "strconv"

// This package centralizes all queue names used across the distributed system
// to ensure consistency and avoid duplication

const (
	// ============================================================================
	// Client Communication Queues
	// ============================================================================
	ClientResultsQueue = "client-results-queue"

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
	StoreIdChunkQueue      = "storeid-join-chunks"

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
	// Group By Queues (consumed by partitioners) // TODO: Could use an exchange
	Query2GroupByQueue = "query2-groupby-queue"
	Query3GroupByQueue = "query3-groupby-queue"
	Query4GroupByQueue = "query4-groupby-queue"

	// Group By Orchestrator Queues and Exchanges
	Query2OrchestratorChunksQueue = "query2-orchestrator-chunks"
	Query3OrchestratorChunksQueue = "query3-orchestrator-chunks"
	Query4OrchestratorChunksQueue = "query4-orchestrator-chunks"

	// Group By Worker Exchanges (Partitioner -> Worker)
	Query2GroupByExchange = "query2-groupby-exchange"
	Query3GroupByExchange = "query3-groupby-exchange"
	Query4GroupByExchange = "query4-groupby-exchange"

	// Query 2 Routing Keys
	Query2RoutingKeyS2_2023 = "query2.semester.2.2023"
	Query2RoutingKeyS1_2024 = "query2.semester.1.2024"
	Query2RoutingKeyS2_2024 = "query2.semester.2.2024"
	Query2RoutingKeyS1_2025 = "query2.semester.1.2025"
	Query2RoutingKeyS2_2025 = "query2.semester.2.2025"

	// Query 3 Routing Keys
	Query3RoutingKeyS2_2023 = "query3.semester.2.2023"
	Query3RoutingKeyS1_2024 = "query3.semester.1.2024"
	Query3RoutingKeyS2_2024 = "query3.semester.2.2024"
	Query3RoutingKeyS1_2025 = "query3.semester.1.2025"
	Query3RoutingKeyS2_2025 = "query3.semester.2.2025"

	// Group By Results Queues
	Query2GroupByResultsQueue = Query2TopItemsQueue
	Query3GroupByResultsQueue = StoreIdChunkQueue
	Query4GroupByResultsQueue = Query4TopUsersQueue

	// Top Classification Queues
	Query2TopItemsQueue = "query2-top-items-queue"
	Query4TopUsersQueue = "query4-top-users-queue"


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
	// Results Dispatcher Queues
	// ============================================================================
	ResultsDispatcherQueue = "streaming-service-queue"

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

// GetGroupByExchangeName returns the exchange name for group by workers for a specific query
func GetGroupByExchangeName(queryType int) string {
	switch queryType {
	case 2:
		return Query2GroupByExchange
	case 3:
		return Query3GroupByExchange
	case 4:
		return Query4GroupByExchange
	default:
		return ""
	}
}

// GetGroupByWorkerRoutingKey returns the routing key for a specific worker
func GetGroupByWorkerRoutingKey(queryType int, workerID int) string {
	workerIDStr := strconv.Itoa(workerID)
	switch queryType {
	case 2:
		return "query2.worker." + workerIDStr
	case 3:
		return "query3.worker." + workerIDStr
	case 4:
		return "query4.worker." + workerIDStr
	default:
		return ""
	}
}
