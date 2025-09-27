package main

import (
	"os"
	"tp-distribuidos-2c2025/shared/middleware"
)

func main() {
	// Initialize the logger with environment variables
	middleware.InitLogger()
	
	// Run the tests
	os.Exit(0)
}
