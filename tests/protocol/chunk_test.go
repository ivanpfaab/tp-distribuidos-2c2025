package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chunk "tp-distribuidos-2c2025/protocol/chunk"
	"tp-distribuidos-2c2025/protocol/common"
)

func TestNewChunk(t *testing.T) {
	clientID := "1234"
	queryType := uint8(1)
	chunkNumber := 5
	isLastChunk := true
	step := 2
	chunkSize := 100
	tableID := 3
	chunkData := "test chunk data"

	c := chunk.NewChunk(clientID, queryType, chunkNumber, isLastChunk, step, chunkSize, tableID, chunkData)

	assert.Equal(t, clientID, c.ClientID)
	assert.Equal(t, queryType, c.QueryType)
	assert.Equal(t, chunkNumber, c.ChunkNumber)
	assert.Equal(t, isLastChunk, c.IsLastChunk)
	assert.Equal(t, step, c.Step)
	assert.Equal(t, chunkSize, c.ChunkSize)
	assert.Equal(t, tableID, c.TableID)
	assert.Equal(t, chunkData, c.ChunkData)
}

func TestNewChunkMessage(t *testing.T) {
	clientID := "1234"
	queryType := uint8(2)
	chunkNumber := 3
	isLastChunk := false
	step := 1
	chunkSize := 50
	tableID := 1
	chunkData := "chunk data"

	c := chunk.NewChunk(clientID, queryType, chunkNumber, isLastChunk, step, chunkSize, tableID, chunkData)
	msg := chunk.NewChunkMessage(c)

	assert.Equal(t, uint16(0), msg.Header.HeaderLength) // Will be calculated during serialization
	assert.Equal(t, int32(0), msg.Header.TotalLength)   // Will be calculated during serialization
	assert.Equal(t, common.ChunkMessageType, msg.Header.MsgTypeID)
	assert.Equal(t, clientID, msg.Chunk.ClientID)
	assert.Equal(t, queryType, msg.Chunk.QueryType)
	assert.Equal(t, chunkNumber, msg.Chunk.ChunkNumber)
	assert.Equal(t, isLastChunk, msg.Chunk.IsLastChunk)
	assert.Equal(t, step, msg.Chunk.Step)
	assert.Equal(t, chunkSize, msg.Chunk.ChunkSize)
	assert.Equal(t, tableID, msg.Chunk.TableID)
	assert.Equal(t, chunkData, msg.Chunk.ChunkData)
}

func TestSerializeChunkMessage(t *testing.T) {
	tests := []struct {
		name    string
		msg     *chunk.ChunkMessage
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid chunk message",
			msg: &chunk.ChunkMessage{
				Header: common.Header{
					HeaderLength: 0,
					TotalLength:  0,
					MsgTypeID:    common.ChunkMessageType,
				},
				Chunk: chunk.Chunk{
					ClientID:    "1234",
					QueryType:   1,
					ChunkNumber: 5,
					IsLastChunk: true,
					Step:        2,
					ChunkSize:   100,
					TableID:     3,
					ChunkData:   "test chunk data",
				},
			},
			wantErr: false,
		},
		{
			name: "valid chunk message with false is_last_chunk",
			msg: &chunk.ChunkMessage{
				Header: common.Header{
					HeaderLength: 0,
					TotalLength:  0,
					MsgTypeID:    common.ChunkMessageType,
				},
				Chunk: chunk.Chunk{
					ClientID:    "1234",
					QueryType:   2,
					ChunkNumber: 1,
					IsLastChunk: false,
					Step:        0,
					ChunkSize:   50,
					TableID:     1,
					ChunkData:   "more chunk data",
				},
			},
			wantErr: false,
		},
		{
			name: "client_id too long",
			msg: &chunk.ChunkMessage{
				Header: common.Header{
					HeaderLength: 0,
					TotalLength:  0,
					MsgTypeID:    common.ChunkMessageType,
				},
				Chunk: chunk.Chunk{
					ClientID:    "12345", // 5 bytes, max is 4
					QueryType:   1,
					ChunkNumber: 5,
					IsLastChunk: true,
					Step:        2,
					ChunkSize:   100,
					TableID:     3,
					ChunkData:   "test chunk data",
				},
			},
			wantErr: true,
			errMsg:  "client_id too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := chunk.SerializeChunkMessage(tt.msg)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, data)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, data)
				assert.Greater(t, len(data), 0)
			}
		})
	}
}

func TestSerializeChunkMessageStructure(t *testing.T) {
	msg := &chunk.ChunkMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ChunkMessageType,
		},
		Chunk: chunk.Chunk{
			ClientID:    "1234",
			QueryType:   1,
			ChunkNumber: 5,
			IsLastChunk: true,
			Step:        2,
			ChunkSize:   100,
			TableID:     3,
			ChunkData:   "test chunk data",
		},
	}

	data, err := chunk.SerializeChunkMessage(msg)
	require.NoError(t, err)

	// Verify header structure
	offset := 0

	// Check header_length (2 bytes)
	headerLength := binary.BigEndian.Uint16(data[offset:])
	expectedHeaderLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + chunk.ClientIDSize + chunk.QueryTypeSize + chunk.TableIDSize + chunk.ChunkSizeSize + chunk.ChunkNumberSize + chunk.IsLastChunkSize + chunk.StepSize
	assert.Equal(t, uint16(expectedHeaderLength), headerLength)
	offset += common.HeaderLengthSize

	// Check total_length (4 bytes)
	totalLength := binary.BigEndian.Uint32(data[offset:])
	expectedTotalLength := expectedHeaderLength + len(msg.Chunk.ChunkData)
	assert.Equal(t, uint32(expectedTotalLength), totalLength)
	offset += common.TotalLengthSize

	// Check msg_type_id (1 byte)
	assert.Equal(t, byte(common.ChunkMessageType), data[offset])
	offset += common.MsgTypeIDSize

	// Check client_id (4 bytes)
	assert.Equal(t, []byte("1234"), data[offset:offset+chunk.ClientIDSize])
	offset += chunk.ClientIDSize

	// Check query_type (1 byte)
	assert.Equal(t, uint8(1), data[offset])
	offset += chunk.QueryTypeSize

	// Check table_id (1 byte)
	assert.Equal(t, byte(3), data[offset])
	offset += chunk.TableIDSize

	// Check chunk_size (8 bytes)
	chunkSize := binary.BigEndian.Uint64(data[offset:])
	assert.Equal(t, uint64(100), chunkSize)
	offset += chunk.ChunkSizeSize

	// Check chunk_number (8 bytes)
	chunkNumber := binary.BigEndian.Uint64(data[offset:])
	assert.Equal(t, uint64(5), chunkNumber)
	offset += chunk.ChunkNumberSize

	// Check is_last_chunk (1 byte)
	assert.Equal(t, byte(1), data[offset]) // true should be 1
	offset += chunk.IsLastChunkSize

	// Check step (1 byte)
	assert.Equal(t, byte(2), data[offset])
	offset += chunk.StepSize

	// Check chunk_data (variable length)
	assert.Equal(t, []byte("test chunk data"), data[offset:])
}

func TestSerializeChunkMessageWithEmptyData(t *testing.T) {
	msg := &chunk.ChunkMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ChunkMessageType,
		},
		Chunk: chunk.Chunk{
			ClientID:    "1234",
			QueryType:   1,
			ChunkNumber: 1,
			IsLastChunk: false,
			Step:        0,
			ChunkSize:   0,
			TableID:     1,
			ChunkData:   "", // Empty data
		},
	}

	data, err := chunk.SerializeChunkMessage(msg)
	require.NoError(t, err)

	// Should still serialize successfully with empty data
	assert.NotNil(t, data)
	expectedLength := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + chunk.ClientIDSize + chunk.QueryTypeSize + chunk.TableIDSize + chunk.ChunkSizeSize + chunk.ChunkNumberSize + chunk.IsLastChunkSize + chunk.StepSize
	assert.Equal(t, expectedLength, len(data))
}

func TestQueryTypes(t *testing.T) {
	// Test all valid query types
	validQueryTypes := []uint8{chunk.QueryType1, chunk.QueryType2, chunk.QueryType3, chunk.QueryType4}

	for _, queryType := range validQueryTypes {
		t.Run("query_type_"+string(rune(queryType+'0')), func(t *testing.T) {
			msg := &chunk.ChunkMessage{
				Header: common.Header{
					HeaderLength: 0,
					TotalLength:  0,
					MsgTypeID:    common.ChunkMessageType,
				},
				Chunk: chunk.Chunk{
					ClientID:    "1234",
					QueryType:   queryType,
					ChunkNumber: 1,
					IsLastChunk: false,
					Step:        0,
					ChunkSize:   10,
					TableID:     1,
					ChunkData:   "test",
				},
			}

			data, err := chunk.SerializeChunkMessage(msg)
			require.NoError(t, err)
			assert.NotNil(t, data)
		})
	}
}

func TestChunkMessageWithLargeNumbers(t *testing.T) {
	// Test with large numbers to ensure 64-bit fields work correctly
	msg := &chunk.ChunkMessage{
		Header: common.Header{
			HeaderLength: 0,
			TotalLength:  0,
			MsgTypeID:    common.ChunkMessageType,
		},
		Chunk: chunk.Chunk{
			ClientID:    "1234",
			QueryType:   1,
			ChunkNumber: 9223372036854775807, // Max int64
			IsLastChunk: true,
			Step:        255,                 // Max uint8
			ChunkSize:   9223372036854775807, // Max int64
			TableID:     255,                 // Max uint8
			ChunkData:   "large number test",
		},
	}

	data, err := chunk.SerializeChunkMessage(msg)
	require.NoError(t, err)
	assert.NotNil(t, data)

	// Verify the large numbers were serialized correctly
	offset := common.HeaderLengthSize + common.TotalLengthSize + common.MsgTypeIDSize + chunk.ClientIDSize + chunk.QueryTypeSize + chunk.TableIDSize

	// Check chunk_size
	chunkSize := binary.BigEndian.Uint64(data[offset:])
	assert.Equal(t, uint64(9223372036854775807), chunkSize)
	offset += chunk.ChunkSizeSize

	// Check chunk_number
	chunkNumber := binary.BigEndian.Uint64(data[offset:])
	assert.Equal(t, uint64(9223372036854775807), chunkNumber)
}
