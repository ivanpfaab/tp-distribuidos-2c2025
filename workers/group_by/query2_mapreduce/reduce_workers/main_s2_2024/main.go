package main

import (
	"log"

	"github.com/tp-distribuidos-2c2025/workers/group_by/query2_mapreduce/reduce_workers/shared"
)

func main() {
	// Create reduce worker for S2-2024
	semester := shared.Semester{Year: 2024, Semester: 2}
	reduceWorker := shared.NewReduceWorker(semester)
	defer reduceWorker.Close()

	log.Printf("Starting Reduce Worker for S2-2024...")
	reduceWorker.Start()

	select {}
}
