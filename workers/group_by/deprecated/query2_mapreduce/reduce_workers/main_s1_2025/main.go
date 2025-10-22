package main

import (
	"log"

	"github.com/tp-distribuidos-2c2025/workers/group_by/query2_mapreduce/reduce_workers/shared"
)

func main() {
	// Create reduce worker for S1-2025
	semester := shared.Semester{Year: 2025, Semester: 1}
	reduceWorker := shared.NewReduceWorker(semester)
	defer reduceWorker.Close()

	log.Printf("Starting Reduce Worker for S1-2025...")
	reduceWorker.Start()

	select {}
}
