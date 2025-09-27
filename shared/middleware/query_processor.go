package middleware

import (
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueryChunk represents a chunk of data to be processed
type QueryChunk struct {
	ID          string      `json:"id"`
	QueryID     string      `json:"query_id"`
	WorkerType  string      `json:"worker_type"` // "filter", "groupby", "join", etc.
	Data        interface{} `json:"data"`
	Sequence    int         `json:"sequence"`
	TotalChunks int         `json:"total_chunks"`
	Source      string      `json:"source"`
	Timestamp   time.Time   `json:"timestamp"`
}

// QueryResult represents the result of processing a chunk
type QueryResult struct {
	ChunkID     string      `json:"chunk_id"`
	QueryID     string      `json:"query_id"`
	WorkerType  string      `json:"worker_type"`
	Result      interface{} `json:"result"`
	Success     bool        `json:"success"`
	Error       string      `json:"error,omitempty"`
	ProcessedAt time.Time   `json:"processed_at"`
	WorkerID    string      `json:"worker_id"`
}

// QueryMaster represents the master that distributes chunks
type QueryMaster struct {
	connection   *amqp.Connection
	channel      *amqp.Channel
	resultsQueue string
	workerTypes  map[string]string // worker_type -> exchange_name
}

// NewQueryMaster creates a new query master
func NewQueryMaster(rabbitMQURL string) (*QueryMaster, error) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Create results queue for receiving processed results
	resultsQueue, err := ch.QueueDeclare(
		"query_results", // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare results queue: %w", err)
	}

	return &QueryMaster{
		connection:   conn,
		channel:      ch,
		resultsQueue: resultsQueue.Name,
		workerTypes:  make(map[string]string),
	}, nil
}

// RegisterWorkerType registers a new type of worker
func (qm *QueryMaster) RegisterWorkerType(workerType, exchangeName string) error {
	// Create exchange for this worker type
	err := qm.channel.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange for %s: %w", workerType, err)
	}

	qm.workerTypes[workerType] = exchangeName
	return nil
}

// SendChunk sends a chunk to the appropriate worker type
func (qm *QueryMaster) SendChunk(chunk *QueryChunk) error {
	exchangeName, exists := qm.workerTypes[chunk.WorkerType]
	if !exists {
		return fmt.Errorf("unknown worker type: %s", chunk.WorkerType)
	}

	// Serialize chunk
	chunkData, err := json.Marshal(chunk)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk: %w", err)
	}

	// Send to appropriate exchange
	err = qm.channel.Publish(
		exchangeName,     // exchange
		chunk.WorkerType, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        chunkData,
			MessageId:   chunk.ID,
			ReplyTo:     qm.resultsQueue, // Where to send results
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish chunk: %w", err)
	}

	return nil
}

// StartReceivingResults starts listening for results from workers
func (qm *QueryMaster) StartReceivingResults(onResult func(*QueryResult)) error {
	msgs, err := qm.channel.Consume(
		qm.resultsQueue, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go func() {
		for msg := range msgs {
			var result QueryResult
			if err := json.Unmarshal(msg.Body, &result); err != nil {
				// Handle error
				msg.Nack(false, false)
				continue
			}

			onResult(&result)
			msg.Ack(false)
		}
	}()

	return nil
}

// QueryWorker represents a worker that processes chunks
type QueryWorker struct {
	workerType     string
	workerID       string
	connection     *amqp.Connection
	channel        *amqp.Channel
	consumeChannel <-chan amqp.Delivery
	processor      func(*QueryChunk) (*QueryResult, error)
}

// NewQueryWorker creates a new query worker
func NewQueryWorker(rabbitMQURL, workerType, workerID string, processor func(*QueryChunk) (*QueryResult, error)) (*QueryWorker, error) {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Create exchange for this worker type
	exchangeName := fmt.Sprintf("workers_%s", workerType)
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Create queue for this worker
	queueName := fmt.Sprintf("worker_%s_%s", workerType, workerID)
	queue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		queue.Name,   // queue name
		workerType,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to bind queue: %w", err)
	}

	return &QueryWorker{
		workerType: workerType,
		workerID:   workerID,
		connection: conn,
		channel:    ch,
		processor:  processor,
	}, nil
}

// StartProcessing starts processing chunks
func (qw *QueryWorker) StartProcessing() error {
	msgs, err := qw.channel.Consume(
		fmt.Sprintf("worker_%s_%s", qw.workerType, qw.workerID), // queue
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	qw.consumeChannel = msgs

	go func() {
		for msg := range msgs {
			var chunk QueryChunk
			if err := json.Unmarshal(msg.Body, &chunk); err != nil {
				// Handle error
				msg.Nack(false, false)
				continue
			}

			// Process the chunk
			result, err := qw.processor(&chunk)
			if err != nil {
				result = &QueryResult{
					ChunkID:     chunk.ID,
					QueryID:     chunk.QueryID,
					WorkerType:  qw.workerType,
					Success:     false,
					Error:       err.Error(),
					ProcessedAt: time.Now(),
					WorkerID:    qw.workerID,
				}
			}

			// Send result back to master
			resultData, err := json.Marshal(result)
			if err != nil {
				msg.Nack(false, false)
				continue
			}

			// Send result to the reply-to queue
			err = qw.channel.Publish(
				"",          // exchange
				msg.ReplyTo, // routing key (results queue)
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        resultData,
					MessageId:   result.ChunkID,
				},
			)

			if err != nil {
				// Handle error
				msg.Nack(false, false)
				continue
			}

			msg.Ack(false)
		}
	}()

	return nil
}

// Close closes the worker
func (qw *QueryWorker) Close() error {
	if qw.channel != nil {
		qw.channel.Close()
	}
	if qw.connection != nil {
		return qw.connection.Close()
	}
	return nil
}

// Close closes the master
func (qm *QueryMaster) Close() error {
	if qm.channel != nil {
		qm.channel.Close()
	}
	if qm.connection != nil {
		return qm.connection.Close()
	}
	return nil
}

