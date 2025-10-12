package main

import (
	"fmt"
	"log"

	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/protocol/deserializer"
	"github.com/tp-distribuidos-2c2025/shared/middleware"
	"github.com/tp-distribuidos-2c2025/shared/middleware/workerqueue"
)

// DummyGroupByWorker handles Query 3 and Query 4 as simple pass-through
type DummyGroupByWorker struct {
	consumer *workerqueue.QueueConsumer
	producer *workerqueue.QueueMiddleware
	config   *middleware.ConnectionConfig
}

// NewDummyGroupByWorker creates a new dummy groupby worker for Query 3 and 4
func NewDummyGroupByWorker() *DummyGroupByWorker {
	config := &middleware.ConnectionConfig{
		Host:     "rabbitmq",
		Port:     5672,
		Username: "admin",
		Password: "password",
	}

	// Create consumer for storeid-groupby-chunks (Query 3 and 4)
	consumer := workerqueue.NewQueueConsumer("storeid-groupby-chunks", config)
	if consumer == nil {
		log.Fatal("Failed to create consumer for storeid-groupby-chunks")
	}

	// Create producer for final results
	producer := workerqueue.NewMessageMiddlewareQueue("storeid-metrics-chunks", config)
	if producer == nil {
		log.Fatal("Failed to create producer for storeid-metrics-chunks")
	}

	return &DummyGroupByWorker{
		consumer: consumer,
		producer: producer,
		config:   config,
	}
}

// Start starts the dummy groupby worker
func (dgw *DummyGroupByWorker) Start() {
	log.Println("Starting Dummy GroupBy Worker for Query 3 and 4...")
	log.Println("This worker simply passes through chunks without grouping")

	onMessageCallback := func(consumeChannel middleware.ConsumeChannel, done chan error) {
		for delivery := range *consumeChannel {
			// Deserialize the chunk message
			message, err := deserializer.Deserialize(delivery.Body)
			if err != nil {
				log.Printf("Failed to deserialize chunk: %v", err)
				delivery.Ack(false)
				continue
			}

			// Check if it's a Chunk message
			chunkData, ok := message.(*chunk.Chunk)
			if !ok {
				log.Printf("Received non-chunk message: %T", message)
				delivery.Ack(false)
				continue
			}

			// Only handle Query 3 and 4
			if chunkData.QueryType != 3 && chunkData.QueryType != 4 {
				log.Printf("Received non-Query 3/4 chunk: QueryType=%d, skipping", chunkData.QueryType)
				delivery.Ack(false)
				continue
			}

			log.Printf("Passing through chunk for QueryType %d, ChunkNumber %d, IsLastChunk %v",
				chunkData.QueryType, chunkData.ChunkNumber, chunkData.IsLastChunk)

			// Simply forward the chunk to the output queue
			chunkMsg := chunk.NewChunkMessage(chunkData)
			serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
			if err != nil {
				log.Printf("Failed to serialize chunk: %v", err)
				delivery.Ack(false)
				continue
			}

			sendErr := dgw.producer.Send(serializedData)
			if sendErr != 0 {
				log.Printf("Failed to send chunk: error code %v", sendErr)
				delivery.Ack(false)
				continue
			}

			log.Printf("Successfully passed through chunk %d for QueryType %d",
				chunkData.ChunkNumber, chunkData.QueryType)

			// Acknowledge the message
			delivery.Ack(false)
		}
		done <- nil
	}

	if err := dgw.consumer.StartConsuming(onMessageCallback); err != 0 {
		log.Fatalf("Failed to start consuming: %v", err)
	}
}

// Close closes the dummy groupby worker
func (dgw *DummyGroupByWorker) Close() {
	if dgw.consumer != nil {
		dgw.consumer.Close()
	}

	if dgw.producer != nil {
		dgw.producer.Close()
	}
}

func main() {
	fmt.Println("GroupBy Worker: Starting")
	fmt.Println("Note: Query 2 uses dedicated MapReduce workers")
	fmt.Println("Note: Query 3 and 4 use this dummy pass-through worker")

	dummyWorker := NewDummyGroupByWorker()
	defer dummyWorker.Close()

	dummyWorker.Start()

	// Keep the worker running
	select {}
}
