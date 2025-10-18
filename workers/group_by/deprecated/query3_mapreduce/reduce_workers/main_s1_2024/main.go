package main

import (
	"log"

	"github.com/tp-distribuidos-2c2025/workers/group_by/query3_mapreduce/reduce_workers/shared"
)

func main() {
	// Create reduce worker for S1-2024
	semester := shared.Semester{Year: 2024, Semester: 1}
	reduceWorker := shared.NewReduceWorker(semester)
	defer reduceWorker.Close()

	log.Printf("Starting Reduce Worker for S1-2024 (Query 3)...")
	reduceWorker.Start()

	select {}
}
