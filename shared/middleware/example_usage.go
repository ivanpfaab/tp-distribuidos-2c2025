package middleware

import (
	"fmt"
	"log"
	"time"
)

// Example usage of the query processing system
func ExampleQueryProcessing() {
	rabbitMQURL := "amqp://guest:guest@localhost:5672/"

	// 1. Create Query Master
	master, err := NewQueryMaster(rabbitMQURL)
	if err != nil {
		log.Fatal("Failed to create master:", err)
	}
	defer master.Close()

	// 2. Register worker types
	master.RegisterWorkerType("filter", "workers_filter")
	master.RegisterWorkerType("groupby", "workers_groupby")
	master.RegisterWorkerType("join", "workers_join")

	// 3. Create workers
	filterWorker1, err := NewQueryWorker(rabbitMQURL, "filter", "worker1", processFilterChunk)
	if err != nil {
		log.Fatal("Failed to create filter worker 1:", err)
	}
	defer filterWorker1.Close()

	filterWorker2, err := NewQueryWorker(rabbitMQURL, "filter", "worker2", processFilterChunk)
	if err != nil {
		log.Fatal("Failed to create filter worker 2:", err)
	}
	defer filterWorker2.Close()

	groupbyWorker1, err := NewQueryWorker(rabbitMQURL, "groupby", "worker1", processGroupByChunk)
	if err != nil {
		log.Fatal("Failed to create groupby worker 1:", err)
	}
	defer groupbyWorker1.Close()

	// 4. Start workers
	filterWorker1.StartProcessing()
	filterWorker2.StartProcessing()
	groupbyWorker1.StartProcessing()

	// 5. Start receiving results
	master.StartReceivingResults(func(result *QueryResult) {
		fmt.Printf("Received result for chunk %s: %+v\n", result.ChunkID, result)
	})

	// 6. Send some chunks
	chunks := []QueryChunk{
		{
			ID:          "chunk1",
			QueryID:     "query1",
			WorkerType:  "filter",
			Data:        map[string]interface{}{"name": "John", "age": 25},
			Sequence:    1,
			TotalChunks: 3,
			Source:      "users",
			Timestamp:   time.Now(),
		},
		{
			ID:          "chunk2",
			QueryID:     "query1",
			WorkerType:  "filter",
			Data:        map[string]interface{}{"name": "Jane", "age": 30},
			Sequence:    2,
			TotalChunks: 3,
			Source:      "users",
			Timestamp:   time.Now(),
		},
		{
			ID:          "chunk3",
			QueryID:     "query1",
			WorkerType:  "groupby",
			Data:        map[string]interface{}{"department": "IT", "salary": 50000},
			Sequence:    3,
			TotalChunks: 3,
			Source:      "employees",
			Timestamp:   time.Now(),
		},
	}

	// Send chunks
	for _, chunk := range chunks {
		err := master.SendChunk(&chunk)
		if err != nil {
			log.Printf("Failed to send chunk %s: %v", chunk.ID, err)
		}
	}

	// Wait for processing
	time.Sleep(5 * time.Second)
}

// Example filter processor
func processFilterChunk(chunk *QueryChunk) (*QueryResult, error) {
	fmt.Printf("Filter worker processing chunk %s\n", chunk.ID)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	// Example filter logic: only keep users with age > 25
	if data, ok := chunk.Data.(map[string]interface{}); ok {
		if age, ok := data["age"].(int); ok && age > 25 {
			return &QueryResult{
				ChunkID:     chunk.ID,
				QueryID:     chunk.QueryID,
				WorkerType:  "filter",
				Result:      data,
				Success:     true,
				ProcessedAt: time.Now(),
				WorkerID:    "filter_worker",
			}, nil
		}
	}

	// Filtered out
	return &QueryResult{
		ChunkID:     chunk.ID,
		QueryID:     chunk.QueryID,
		WorkerType:  "filter",
		Result:      nil,
		Success:     true,
		ProcessedAt: time.Now(),
		WorkerID:    "filter_worker",
	}, nil
}

// Example groupby processor
func processGroupByChunk(chunk *QueryChunk) (*QueryResult, error) {
	fmt.Printf("GroupBy worker processing chunk %s\n", chunk.ID)

	// Simulate processing time
	time.Sleep(150 * time.Millisecond)

	// Example groupby logic
	result := map[string]interface{}{
		"department": "IT",
		"count":      1,
		"avg_salary": 50000,
	}

	return &QueryResult{
		ChunkID:     chunk.ID,
		QueryID:     chunk.QueryID,
		WorkerType:  "groupby",
		Result:      result,
		Success:     true,
		ProcessedAt: time.Now(),
		WorkerID:    "groupby_worker",
	}, nil
}

