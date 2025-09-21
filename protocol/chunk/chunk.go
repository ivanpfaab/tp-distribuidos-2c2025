package chunk

type Chunk struct {
	ClientID     string
	QueryType    uint8
	ChunkNumber  int
	IsLastChunk  bool
	Step         int
	ChunkSize    int
	TableID      int
	ChunkData    string
}

func NewChunk(clientID string, queryType uint8, chunkNumber int, isLastChunk bool, step, chunkSize, tableID int, chunkData string) *Chunk {
	return &Chunk{
		ClientID:    clientID,
		QueryType:   queryType,
		ChunkNumber: chunkNumber,
		IsLastChunk: isLastChunk,
		Step:        step,
		ChunkSize:   chunkSize,
		TableID:     tableID,
		ChunkData:   chunkData,
	}
}
