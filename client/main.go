package main

import (
	"bufio"
	"encoding/csv"
	"flag"
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
	batchSize := flag.Int("batch", 10, "Number of CSV records per batch")
	addr := flag.String("addr", "server:8080", "Server address host:port")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("Usage: ./client [-batch N] [-addr host:port] <input.csv>")
	}
	inputFile := flag.Arg(0)

	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open CSV: %v", err)
	}
	defer f.Close()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	fmt.Printf("Connected to %s\n", *addr)
	fmt.Printf("Reading CSV: %s | batch size: %d\n", inputFile, *batchSize)

	// Start goroutine for server responses
	startServerReader(conn)

	// Read CSV and send batches
	r := csv.NewReader(f)
	records, batches, err := sendBatches(conn, r, *batchSize)
	if err != nil {
		log.Fatalf("Transmission error: %v", err)
	}

	fmt.Printf("Finished sending %d record(s) in %d batch(es)\n", records, batches)
}
