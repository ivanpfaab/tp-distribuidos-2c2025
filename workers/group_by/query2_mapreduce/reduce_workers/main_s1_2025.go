package main

import (
	"log"
)

func main() {
	// Create reduce worker for S1-2025
	semester := Semester{Year: 2025, Semester: 1}
	reduceWorker := NewReduceWorker(semester)
	defer reduceWorker.Close()
	
	log.Printf("Starting Reduce Worker for S1-2025...")
	reduceWorker.Start()	

	select {}
}
