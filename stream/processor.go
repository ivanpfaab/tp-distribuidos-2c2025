package main

import (
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// processMessage processes incoming messages and prints the results
func (sw *StreamingWorker) processMessage(delivery amqp.Delivery) middleware.MessageMiddlewareError {
	// Deserialize the chunk message
	chunkFromMsg, err := deserializer.Deserialize(delivery.Body)
	if err != nil {
		testing.LogError("Streaming Worker", "Failed to deserialize chunk message: %v", err)
		return middleware.MessageMiddlewareMessageError
	}

	chunkData, ok := chunkFromMsg.(*chunk.Chunk)
	if !ok {
		testing.LogError("Streaming Worker", "Failed to cast message to chunk")
		return middleware.MessageMiddlewareMessageError
	}

	// Check if this is a query that needs final filtering (Query 2 or Query 4)
	if chunkData.QueryType == 2 || chunkData.QueryType == 4 {
		return sw.handleFinalFilteringQuery(chunkData)
	}

	// For other queries, print results immediately
	sw.printResult(chunkData)
	return 0
}

// handleFinalFilteringQuery handles queries that need final filtering (Query 2 and Query 4)
func (sw *StreamingWorker) handleFinalFilteringQuery(chunkData *chunk.Chunk) middleware.MessageMiddlewareError {
	clientID := chunkData.ClientID

	// Initialize tracking for this client if not exists
	if _, exists := sw.collectedChunks[clientID]; !exists {
		sw.collectedChunks[clientID] = make([]string, 0)
		sw.chunkCounts[clientID] = 0
		sw.receivedCounts[clientID] = 0
	}

	// Collect this chunk's data
	sw.collectedChunks[clientID] = append(sw.collectedChunks[clientID], chunkData.ChunkData)
	sw.receivedCounts[clientID]++

	// If this is the last chunk, we need to determine total chunks expected
	// For now, we'll process when we receive a chunk marked as last
	if chunkData.IsLastChunk {
		fmt.Printf("Streaming Worker: Received last chunk for client %s, processing final filtering for Query %d\n",
			clientID, chunkData.QueryType)

		// Process final filtering based on query type
		if chunkData.QueryType == 2 {
			sw.processQuery2FinalFiltering(clientID)
		} else if chunkData.QueryType == 4 {
			sw.processQuery4FinalFiltering(clientID)
		}

		// Clean up tracking data for this client
		delete(sw.collectedChunks, clientID)
		delete(sw.chunkCounts, clientID)
		delete(sw.receivedCounts, clientID)
	}

	return 0
}

// processQuery2FinalFiltering processes Query 2 final filtering: Top item by quantity and top item by profits per month
func (sw *StreamingWorker) processQuery2FinalFiltering(clientID string) {
	fmt.Printf("Streaming Worker: Processing Query 2 final filtering for client %s\n", clientID)

	// Combine all chunks
	var allData strings.Builder
	for i, chunkData := range sw.collectedChunks[clientID] {
		fmt.Printf("Streaming Worker: Chunk %d data length: %d\n", i, len(chunkData))
		fmt.Printf("Streaming Worker: Chunk %d data preview: %s\n", i, chunkData[:min(200, len(chunkData))])
		allData.WriteString(chunkData)
	}

	fmt.Printf("Streaming Worker: Total combined data length: %d\n", allData.Len())
	fmt.Printf("Streaming Worker: Combined data preview: %s\n", allData.String()[:min(500, allData.Len())])

	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(allData.String()))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Streaming Worker: Error parsing CSV for Query 2: %v\n", err)
		return
	}

	fmt.Printf("Streaming Worker: Parsed %d records for Query 2\n", len(records))

	if len(records) < 2 {
		fmt.Printf("Streaming Worker: No data records found for Query 2\n")
		return
	}

	// Skip header row
	dataRecords := records[1:]

	// Group by month and track items
	monthItems := make(map[string][]map[string]string) // month -> []item

	for _, record := range dataRecords {
		if len(record) < 10 {
			continue
		}

		month := record[1] // month column
		item := map[string]string{
			"year":        record[0],
			"month":       record[1],
			"item_id":     record[2],
			"quantity":    record[3],
			"subtotal":    record[4],
			"count":       record[5],
			"item_name":   record[6],
			"category":    record[7],
			"price":       record[8],
			"is_seasonal": record[9],
		}

		monthItems[month] = append(monthItems[month], item)
	}

	// Process each month to find top items
	fmt.Printf("Q2 | year,month,item_id,quantity,subtotal,count,item_name,category,price,is_seasonal,ranking_type\n")

	for _, items := range monthItems {
		// Find top item by quantity
		topByQuantity := sw.findTopItemByQuantity(items)
		if topByQuantity != nil {
			fmt.Printf("Q2 | %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,quantity\n",
				topByQuantity["year"], topByQuantity["month"], topByQuantity["item_id"],
				topByQuantity["quantity"], topByQuantity["subtotal"], topByQuantity["count"],
				topByQuantity["item_name"], topByQuantity["category"], topByQuantity["price"],
				topByQuantity["is_seasonal"])
		}

		// Find top item by profits (subtotal)
		topByProfits := sw.findTopItemByProfits(items)
		if topByProfits != nil {
			fmt.Printf("Q2 | %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,profits\n",
				topByProfits["year"], topByProfits["month"], topByProfits["item_id"],
				topByProfits["quantity"], topByProfits["subtotal"], topByProfits["count"],
				topByProfits["item_name"], topByProfits["category"], topByProfits["price"],
				topByProfits["is_seasonal"])
		}
	}
}

// processQuery4FinalFiltering processes Query 4 final filtering: Top 3 users with most transactions
func (sw *StreamingWorker) processQuery4FinalFiltering(clientID string) {
	fmt.Printf("Streaming Worker: Processing Query 4 final filtering for client %s\n", clientID)

	// Combine all chunks
	var allData strings.Builder
	for i, chunkData := range sw.collectedChunks[clientID] {
		fmt.Printf("Streaming Worker: Chunk %d data length: %d\n", i, len(chunkData))
		fmt.Printf("Streaming Worker: Chunk %d data preview: %s\n", i, chunkData[:min(200, len(chunkData))])
		allData.WriteString(chunkData)
	}

	fmt.Printf("Streaming Worker: Total combined data length: %d\n", allData.Len())
	fmt.Printf("Streaming Worker: Combined data preview: %s\n", allData.String()[:min(500, allData.Len())])

	// Parse CSV data
	reader := csv.NewReader(strings.NewReader(allData.String()))
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Streaming Worker: Error parsing CSV for Query 4: %v\n", err)
		return
	}

	fmt.Printf("Streaming Worker: Parsed %d records for Query 4\n", len(records))

	if len(records) < 2 {
		fmt.Printf("Streaming Worker: No data records found for Query 4\n")
		return
	}

	// Skip header row
	dataRecords := records[1:]

	// Aggregate transactions by user_id
	userTransactions := make(map[string]int) // user_id -> total_count

	for _, record := range dataRecords {
		if len(record) < 3 {
			continue
		}

		userID := record[0]   // user_id column
		countStr := record[2] // count column

		count, err := strconv.Atoi(countStr)
		if err != nil {
			continue
		}

		userTransactions[userID] += count
	}

	// Convert to slice and sort by transaction count
	type userCount struct {
		userID string
		count  int
	}

	var users []userCount
	for userID, count := range userTransactions {
		users = append(users, userCount{userID: userID, count: count})
	}

	// Sort by count (descending)
	sort.Slice(users, func(i, j int) bool {
		return users[i].count > users[j].count
	})

	// Print top 3 users
	fmt.Printf("Q4 | user_id,total_transactions\n")
	for i, user := range users {
		if i >= 3 {
			break
		}
		fmt.Printf("Q4 | %s,%d\n", user.userID, user.count)
	}
}

// findTopItemByQuantity finds the item with highest quantity in a month
func (sw *StreamingWorker) findTopItemByQuantity(items []map[string]string) map[string]string {
	if len(items) == 0 {
		return nil
	}

	var topItem map[string]string
	maxQuantity := 0

	for _, item := range items {
		quantity, err := strconv.Atoi(item["quantity"])
		if err != nil {
			continue
		}

		if quantity > maxQuantity {
			maxQuantity = quantity
			topItem = item
		}
	}

	return topItem
}

// findTopItemByProfits finds the item with highest subtotal (profits) in a month
func (sw *StreamingWorker) findTopItemByProfits(items []map[string]string) map[string]string {
	if len(items) == 0 {
		return nil
	}

	var topItem map[string]string
	maxProfits := 0.0

	for _, item := range items {
		subtotal, err := strconv.ParseFloat(item["subtotal"], 64)
		if err != nil {
			continue
		}

		if subtotal > maxProfits {
			maxProfits = subtotal
			topItem = item
		}
	}

	return topItem
}

// printResult prints the chunk data in a formatted way
func (sw *StreamingWorker) printResult(chunkData *chunk.Chunk) {
	// Split the CSV data into individual rows and print each one
	rows := strings.Split(strings.TrimSpace(chunkData.ChunkData), "\n")
	for _, row := range rows {
		if strings.TrimSpace(row) != "" { // Skip empty rows
			fmt.Printf("Q%d | %s\n", chunkData.QueryType, row)
		}
	}
}
