package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	"github.com/tp-distribuidos-2c2025/shared/queues"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run send_test_data_direct.go <host> <port> <clients>")
		fmt.Println("Example: go run send_test_data_direct.go localhost 5673 CLI1,CLI2,CLI3")
		os.Exit(1)
	}

	host := os.Args[1]
	port := os.Args[2]
	clients := os.Args[3]

	// Connect to RabbitMQ
	conn, err := amqp091.Dial(fmt.Sprintf("amqp://admin:password@%s:%s/", host, port))
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer ch.Close()

	// Use the existing query3-map-queue (where map workers listen)
	// Don't declare it as it already exists with different settings

	// Parse clients (comma-separated)
	clientList := []string{}
	if clients != "" {
		clientList = strings.Split(clients, ",")
		// Trim whitespace from each client ID
		for i, client := range clientList {
			clientList[i] = strings.TrimSpace(client)
		}
	}

	// Generate realistic test data for each client
	for _, client := range clientList {
		fmt.Printf("Sending test data for %s...\n", client)

		// Send data for 2 files (like the real system expects)
		fileIDs := []string{"TR01", "TR02"}

		for _, fileID := range fileIDs {
			fmt.Printf("  Sending data for file %s...\n", fileID)

			// Generate 60+ transactions per file (120 total = 12 chunks of 10)
			transactions := generateTestTransactions(60) // 60 transactions per file

			// Split into chunks of 10 (excluding header from chunking)
			chunkSize := 10
			dataRows := transactions[1:] // Skip header
			totalChunks := (len(dataRows) + chunkSize - 1) / chunkSize

			for chunkNum := 0; chunkNum < totalChunks; chunkNum++ {
				start := chunkNum * chunkSize
				end := start + chunkSize
				if end > len(dataRows) {
					end = len(dataRows)
				}

				// Include header + data rows for this chunk
				chunkData := append([]string{transactions[0]}, dataRows[start:end]...)
				isLastChunk := (chunkNum == totalChunks-1)

				// Create chunk using the protocol
				chunkDataStr := strings.Join(chunkData, "\n")
				chunkObj := chunk.NewChunk(
					client,            // clientID
					fileID,            // fileID (TR01 or TR02)
					3,                 // queryType (Query 3)
					chunkNum+1,        // chunkNumber
					isLastChunk,       // isLastChunk
					0,                 // step
					len(chunkDataStr), // chunkSize
					1,                 // tableID (transactions)
					chunkDataStr,      // chunkData
				)

				// Create chunk message and serialize
				chunkMsg := chunk.NewChunkMessage(chunkObj)
				serializedData, err := chunk.SerializeChunkMessage(chunkMsg)
				if err != nil {
					log.Fatalf("Failed to serialize chunk message: %v", err)
				}

				// Publish to query3-map-queue
				err = ch.Publish(
					"",                    // exchange
					queues.Query3MapQueue, // routing key
					false,                 // mandatory
					false,                 // immediate
					amqp091.Publishing{
						ContentType: "application/octet-stream",
						Body:        serializedData,
					},
				)
				if err != nil {
					log.Fatalf("Failed to publish message: %v", err)
				}

				fmt.Printf("    ✅ Sent chunk %d/%d for %s (file %s) to %s queue\n",
					chunkNum+1, totalChunks, client, fileID, queues.Query3MapQueue)

				// Small delay between chunks
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	fmt.Println("==========================================")
	fmt.Println("Test data sending completed!")
	fmt.Println("==========================================")
}

// generateTestTransactions generates realistic transaction data
func generateTestTransactions(count int) []string {
	// Header
	transactions := []string{
		"transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at",
	}

	// Seed random for consistent data
	rand.Seed(time.Now().UnixNano())

	// Generate transactions
	for i := 1; i <= count; i++ {
		// Generate realistic transaction data
		transactionID := fmt.Sprintf("TXN%06d", i)
		storeID := fmt.Sprintf("STORE%03d", rand.Intn(10)+1)     // 10 stores
		paymentMethodID := fmt.Sprintf("PM%03d", rand.Intn(5)+1) // 5 payment methods
		voucherID := fmt.Sprintf("V%03d", rand.Intn(20)+1)       // 20 vouchers
		userID := fmt.Sprintf("U%06d", rand.Intn(1000)+1)        // 1000 users

		// Realistic amounts
		originalAmount := float64(rand.Intn(500)+10) + rand.Float64()      // $10-$510
		discountApplied := originalAmount * (float64(rand.Intn(30)) / 100) // 0-30% discount
		finalAmount := originalAmount - discountApplied

		// Random date in 2024 (S1-2024 semester)
		year := 2024
		month := rand.Intn(6) + 1 // January-June (S1-2024)
		day := rand.Intn(28) + 1
		hour := rand.Intn(24)
		minute := rand.Intn(60)
		second := rand.Intn(60)

		// Ensure we don't generate invalid dates
		if day > 28 {
			day = 28
		}

		createdAt := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			year, month, day, hour, minute, second)

		transaction := fmt.Sprintf("%s,%s,%s,%s,%s,%.2f,%.2f,%.2f,%s",
			transactionID, storeID, paymentMethodID, voucherID, userID,
			originalAmount, discountApplied, finalAmount, createdAt)

		transactions = append(transactions, transaction)
	}

	return transactions
}
