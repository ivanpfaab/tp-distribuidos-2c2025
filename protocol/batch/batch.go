package batch

// Batch represents data sent by a client
type Batch struct {
	ClientID    string
	FileID      string
	IsEOF       bool
	BatchNumber int
	BatchSize   int
	BatchData   string
}

// NewBatch creates a new Batch instance
func NewBatch(clientID, fileID string, isEOF bool, batchNumber, batchSize int, batchData string) *Batch {
	return &Batch{
		ClientID:    clientID,
		FileID:      fileID,
		IsEOF:       isEOF,
		BatchNumber: batchNumber,
		BatchSize:   batchSize,
		BatchData:   batchData,
	}
}
