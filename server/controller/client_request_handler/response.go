package client_request_handler

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/protocol/batch"
)

// ResponseFormatter formats responses to clients
type ResponseFormatter struct{}

// NewResponseFormatter creates a new response formatter
func NewResponseFormatter() *ResponseFormatter {
	return &ResponseFormatter{}
}

// FormatSuccess creates a success response for a batch message
func (r *ResponseFormatter) FormatSuccess(batchMsg *batch.Batch) []byte {
	response := fmt.Sprintf("ACK: Batch received - ClientID: %s, FileID: %s, BatchNumber: %d\n",
		batchMsg.ClientID, batchMsg.FileID, batchMsg.BatchNumber)
	return []byte(response)
}

// FormatError creates an error response
func (r *ResponseFormatter) FormatError(err error) []byte {
	return []byte("ERROR: " + err.Error())
}

