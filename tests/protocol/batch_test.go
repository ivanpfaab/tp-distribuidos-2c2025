package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batch "tp-distribuidos-2c2025/protocol/batch"
)

func TestNewBatch(t *testing.T) {
	clientID := "1234"
	fileID := "file"
	isEOF := true
	batchNumber := 5
	batchSize := 100
	batchData := "test data"

	b := batch.NewBatch(clientID, fileID, isEOF, batchNumber, batchSize, batchData)

	assert.Equal(t, clientID, b.ClientID)
	assert.Equal(t, fileID, b.FileID)
	assert.Equal(t, isEOF, b.IsEOF)
	assert.Equal(t, batchNumber, b.BatchNumber)
	assert.Equal(t, batchSize, b.BatchSize)
	assert.Equal(t, batchData, b.BatchData)
}

func TestNewBatchMessage(t *testing.T) {
	clientID := "1234"
	fileID := "file"
	isEOF := true
	batchNumber := 5
	batchSize := 100
	batchData := "test data"

	b := batch.NewBatch(clientID, fileID, isEOF, batchNumber, batchSize, batchData)
	msg := batch.NewBatchMessage(b)

	assert.Equal(t, uint16(0), msg.HeaderLength) // Will be calculated during serialization
	assert.Equal(t, int32(0), msg.TotalLength)   // Will be calculated during serialization
	assert.Equal(t, batch.BatchMessageType, msg.MsgTypeID)
	assert.Equal(t, clientID, msg.ClientID)
	assert.Equal(t, fileID, msg.FileID)
	assert.Equal(t, isEOF, msg.IsEOF)
	assert.Equal(t, batchNumber, msg.BatchNumber)
	assert.Equal(t, batchSize, msg.BatchSize)
	assert.Equal(t, batchData, msg.BatchData)
}

func TestSerializeBatchMessage(t *testing.T) {
	tests := []struct {
		name     string
		msg      *batch.BatchMessage
		wantErr  bool
		errMsg   string
	}{
		{
			name: "valid batch message",
			msg: &batch.BatchMessage{
				HeaderLength: 0,
				TotalLength:  0,
				MsgTypeID:    batch.BatchMessageType,
				ClientID:     "1234",
				FileID:       "file",
				IsEOF:        true,
				BatchNumber:  5,
				BatchSize:    100,
				BatchData:    "test data",
			},
			wantErr: false,
		},
		{
			name: "valid batch message with false EOF",
			msg: &batch.BatchMessage{
				HeaderLength: 0,
				TotalLength:  0,
				MsgTypeID:    batch.BatchMessageType,
				ClientID:     "1234",
				FileID:       "file",
				IsEOF:        false,
				BatchNumber:  1,
				BatchSize:    50,
				BatchData:    "more data",
			},
			wantErr: false,
		},
		{
			name: "client_id too long",
			msg: &batch.BatchMessage{
				HeaderLength: 0,
				TotalLength:  0,
				MsgTypeID:    batch.BatchMessageType,
				ClientID:     "12345", // 5 bytes, max is 4
				FileID:       "file",
				IsEOF:        true,
				BatchNumber:  5,
				BatchSize:    100,
				BatchData:    "test data",
			},
			wantErr: true,
			errMsg:  "client_id too long",
		},
		{
			name: "file_id too long",
			msg: &batch.BatchMessage{
				HeaderLength: 0,
				TotalLength:  0,
				MsgTypeID:    batch.BatchMessageType,
				ClientID:     "1234",
				FileID:       "file1", // 5 bytes, max is 4
				IsEOF:        true,
				BatchNumber:  5,
				BatchSize:    100,
				BatchData:    "test data",
			},
			wantErr: true,
			errMsg:  "file_id too long",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := batch.SerializeBatchMessage(tt.msg)

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

func TestSerializeBatchMessageStructure(t *testing.T) {
	msg := &batch.BatchMessage{
		HeaderLength: 0,
		TotalLength:  0,
		MsgTypeID:    batch.BatchMessageType,
		ClientID:     "1234",
		FileID:       "file",
		IsEOF:        true,
		BatchNumber:  5,
		BatchSize:    100,
		BatchData:    "test data",
	}

	data, err := batch.SerializeBatchMessage(msg)
	require.NoError(t, err)

	// Verify header structure
	offset := 0

	// Check header_length (2 bytes)
	headerLength := binary.BigEndian.Uint16(data[offset:])
	expectedHeaderLength := batch.HeaderLengthSize + batch.TotalLengthSize + batch.MsgTypeIDSize + batch.ClientIDSize + batch.FileIDSize + batch.IsEOFSize + batch.BatchNumberSize + batch.BatchSizeSize
	assert.Equal(t, uint16(expectedHeaderLength), headerLength)
	offset += batch.HeaderLengthSize

	// Check total_length (4 bytes)
	totalLength := binary.BigEndian.Uint32(data[offset:])
	expectedTotalLength := expectedHeaderLength + len(msg.BatchData)
	assert.Equal(t, uint32(expectedTotalLength), totalLength)
	offset += batch.TotalLengthSize

	// Check msg_type_id (1 byte)
	assert.Equal(t, byte(batch.BatchMessageType), data[offset])
	offset += batch.MsgTypeIDSize

	// Check client_id (4 bytes)
	assert.Equal(t, []byte("1234"), data[offset:offset+batch.ClientIDSize])
	offset += batch.ClientIDSize

	// Check file_id (4 bytes)
	assert.Equal(t, []byte("file"), data[offset:offset+batch.FileIDSize])
	offset += batch.FileIDSize

	// Check is_eof (1 byte)
	assert.Equal(t, byte(1), data[offset]) // true should be 1
	offset += batch.IsEOFSize

	// Check batch_number (4 bytes)
	batchNumber := binary.BigEndian.Uint32(data[offset:])
	assert.Equal(t, uint32(5), batchNumber)
	offset += batch.BatchNumberSize

	// Check batch_size (4 bytes)
	batchSize := binary.BigEndian.Uint32(data[offset:])
	assert.Equal(t, uint32(100), batchSize)
	offset += batch.BatchSizeSize

	// Check batch_data (variable length)
	assert.Equal(t, []byte("test data"), data[offset:])
}

func TestSerializeBatchMessageWithEmptyData(t *testing.T) {
	msg := &batch.BatchMessage{
		HeaderLength: 0,
		TotalLength:  0,
		MsgTypeID:    batch.BatchMessageType,
		ClientID:     "1234",
		FileID:       "file",
		IsEOF:        false,
		BatchNumber:  1,
		BatchSize:    0,
		BatchData:    "", // Empty data
	}

	data, err := batch.SerializeBatchMessage(msg)
	require.NoError(t, err)

	// Should still serialize successfully with empty data
	assert.NotNil(t, data)
	expectedLength := batch.HeaderLengthSize + batch.TotalLengthSize + batch.MsgTypeIDSize + batch.ClientIDSize + batch.FileIDSize + batch.IsEOFSize + batch.BatchNumberSize + batch.BatchSizeSize
	assert.Equal(t, expectedLength, len(data))
}