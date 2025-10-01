package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func writeAll(w net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			data = data[n:]
			if _, ok := err.(net.Error); ok {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		data = data[n:]
	}
	return nil
}

// startServerReader launches a goroutine that prints responses from the server.
func startServerReader(conn net.Conn) {
	go func() {
		sc := bufio.NewScanner(conn)
		sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
		for sc.Scan() {
			fmt.Printf("Server: %s\n", sc.Text())
		}
		if err := sc.Err(); err != nil && err != io.EOF {
			log.Printf("Error reading from server: %v", err)
		}
	}()
}

// sendBatches reads CSV records, groups them into batches, and sends them.
func sendBatches(conn net.Conn, r *csv.Reader, batchSize int) (int, int, error) {
	r.FieldsPerRecord = -1 // allow variable columns

	var batch []string
	recordNum := 0
	batchNum := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return recordNum, batchNum, fmt.Errorf("CSV read error on record %d: %v", recordNum+1, err)
		}
		recordNum++

		line := strings.Join(rec, ",")
		batch = append(batch, line)

		if len(batch) == batchSize {
			batchNum++
			payload := strings.Join(batch, "\n") + "\n"
			if err := writeAll(conn, []byte(payload)); err != nil {
				return recordNum, batchNum, fmt.Errorf("failed to send batch %d: %w", batchNum, err)
			}
			fmt.Printf("Sent batch %d with %d records\n", batchNum, len(batch))
			batch = batch[:0]
		}
	}

	// Send final partial batch
	if len(batch) > 0 {
		batchNum++
		payload := strings.Join(batch, "\n") + "\n"
		if err := writeAll(conn, []byte(payload)); err != nil {
			return recordNum, batchNum, fmt.Errorf("failed to send final batch %d: %w", batchNum, err)
		}
		fmt.Printf("Sent final batch %d with %d records\n", batchNum, len(batch))
	}

	return recordNum, batchNum, nil
}

func main() {
	// Check if input file is provided
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./client <input_file.txt> [server_address]")
	}

	inputFile := os.Args[1]

	// Get server address from command line or use default
	serverAddr := "localhost:8080"
	if len(os.Args) >= 3 {
		serverAddr = os.Args[2]
	}

	// Run the client
	err := runClient(inputFile, serverAddr)
	if err != nil {
		log.Fatalf("Client error: %v", err)
	}
}
