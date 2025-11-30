package signals

// Size constants used across signal types
const (
	ClientIDSize     = 4
	QueryTypeSize    = 1
	MapWorkerIDSize  = 32
	FileIDSize       = 4
	TableIDSize      = 1
	ChunkNumberSize  = 4
	ResourceTypeSize = 32
	IDSize           = 16 // ClientIDSize (4) + FileIDSize (4) + ChunkNumberSize (8)
)
