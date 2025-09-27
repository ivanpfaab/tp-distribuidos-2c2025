package middleware

import (
	"fmt"
	"os"
	"strings"
)

// LogLevel represents the logging level
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	QUIET
)

var currentLogLevel LogLevel = INFO

// SetLogLevel sets the current logging level
func SetLogLevel(level LogLevel) {
	currentLogLevel = level
}

// SetLogLevelFromString sets the log level from a string
func SetLogLevelFromString(level string) {
	switch strings.ToLower(level) {
	case "debug":
		currentLogLevel = DEBUG
	case "info":
		currentLogLevel = INFO
	case "warn", "warning":
		currentLogLevel = WARN
	case "error":
		currentLogLevel = ERROR
	case "quiet":
		currentLogLevel = QUIET
	default:
		currentLogLevel = INFO
	}
}

// shouldLog checks if a message should be logged at the given level
func shouldLog(level LogLevel) bool {
	return level >= currentLogLevel
}

// LogDebug logs a debug message
func LogDebug(component, message string, args ...interface{}) {
	if shouldLog(DEBUG) {
		fmt.Printf("[DEBUG] %s: %s\n", component, fmt.Sprintf(message, args...))
	}
}

// LogInfo logs an info message
func LogInfo(component, message string, args ...interface{}) {
	if shouldLog(INFO) {
		fmt.Printf("[INFO] %s: %s\n", component, fmt.Sprintf(message, args...))
	}
}

// LogWarn logs a warning message
func LogWarn(component, message string, args ...interface{}) {
	if shouldLog(WARN) {
		fmt.Printf("[WARN] %s: %s\n", component, fmt.Sprintf(message, args...))
	}
}

// LogError logs an error message
func LogError(component, message string, args ...interface{}) {
	if shouldLog(ERROR) {
		fmt.Printf("[ERROR] %s: %s\n", component, fmt.Sprintf(message, args...))
	}
}

// LogTest logs a test-specific message (always shown)
func LogTest(message string, args ...interface{}) {
	fmt.Printf("🧪 %s\n", fmt.Sprintf(message, args...))
}

// LogSuccess logs a success message (always shown)
func LogSuccess(message string, args ...interface{}) {
	fmt.Printf("✅ %s\n", fmt.Sprintf(message, args...))
}

// LogFailure logs a failure message (always shown)
func LogFailure(message string, args ...interface{}) {
	fmt.Printf("❌ %s\n", fmt.Sprintf(message, args...))
}

// LogStep logs a test step message (always shown)
func LogStep(message string, args ...interface{}) {
	fmt.Printf("  → %s\n", fmt.Sprintf(message, args...))
}

// InitLogger initializes the logger with environment variables
func InitLogger() {
	// Check for LOG_LEVEL environment variable
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		SetLogLevelFromString(logLevel)
	}
}
