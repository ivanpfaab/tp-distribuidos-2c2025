package main

import (
	"log"
)

func main() {
	// Create reduce worker for S2-2025
	semester := Semester{Year: 2025, Semester: 2}
	reduceWorker := NewReduceWorker(semester)
	defer reduceWorker.Close()
	
	log.Printf("Starting Reduce Worker for S2-2025...")
	reduceWorker.Start()
}
