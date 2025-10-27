package testing

import (
	"fmt"
	"os"
	"strings"
	"time"
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

const (
	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Gray   = "\033[90m"
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

// getTimestamp returns the current timestamp in a consistent format
func getTimestamp() string {
	return time.Now().Format("2006/01/02 15:04:05")
}

// LogDebug logs a debug message
func LogDebug(component, message string, args ...interface{}) {
	if shouldLog(DEBUG) {
		fmt.Printf("%s [DEBUG] %s: %s\n", getTimestamp(), component, fmt.Sprintf(message, args...))
	}
}

// LogInfo logs an info message
func LogInfo(component, message string, args ...interface{}) {
	if shouldLog(INFO) {
		fmt.Printf("%s [INFO] %s: %s\n", getTimestamp(), component, fmt.Sprintf(message, args...))
	}
}

// LogWarn logs a warning message
func LogWarn(component, message string, args ...interface{}) {
	if shouldLog(WARN) {
		fmt.Printf("%s %s[WARN] %s: %s%s\n", getTimestamp(), Yellow, component, fmt.Sprintf(message, args...), Reset)
	}
}

// LogError logs an error message
func LogError(component, message string, args ...interface{}) {
	if shouldLog(ERROR) {
		fmt.Printf("%s %s[ERROR] %s: %s%s\n", getTimestamp(), Red, component, fmt.Sprintf(message, args...), Reset)
	}
}

// LogTest logs a test-specific message (always shown)
func LogTest(message string, args ...interface{}) {
	fmt.Printf("%s %s[TEST] %s%s\n", getTimestamp(), Blue, fmt.Sprintf(message, args...), Reset)
}

// LogSuccess logs a success message (always shown)
func LogSuccess(message string, args ...interface{}) {
	fmt.Printf("%s %s[SUCCESS] %s%s\n", getTimestamp(), Green, fmt.Sprintf(message, args...), Reset)
}

// LogFailure logs a failure message (always shown)
func LogFailure(message string, args ...interface{}) {
	fmt.Printf("%s %s[FAILURE] %s%s\n", getTimestamp(), Red, fmt.Sprintf(message, args...), Reset)
}

// LogStep logs a test step message (always shown)
func LogStep(message string, args ...interface{}) {
	fmt.Printf("%s   → %s\n", getTimestamp(), fmt.Sprintf(message, args...))
}

// LogQuiet logs a quiet message (always shown, grey color)
func LogQuiet(message string, args ...interface{}) {
	fmt.Printf("%s %s%s%s\n", getTimestamp(), Gray, fmt.Sprintf(message, args...), Reset)
}

// InitLogger initializes the logger with environment variables
func InitLogger() {
	// Check for LOG_LEVEL environment variable
	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		SetLogLevelFromString(logLevel)
	}
}
