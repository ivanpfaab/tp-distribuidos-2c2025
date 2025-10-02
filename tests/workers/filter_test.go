package workers

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tp-distribuidos-2c2025/protocol/chunk"
	testing_utils "github.com/tp-distribuidos-2c2025/shared/testing"
	"github.com/tp-distribuidos-2c2025/workers/filter"
)

const InputSize = 21

func TestWorkerFilter(t *testing.T) {
	testing_utils.InitLogger()
	testing_utils.LogTest("Testing Filter")

	// Read input csv
	inputData, err := os.ReadFile("test_input.csv")
	if err != nil {
		t.Fatalf("Failed to read input file: %v", err)
	}
	testing_utils.LogStep("Input data: %s", string(inputData))
	result, filterErr := filter.FilterLogic(1, chunk.QueryType1, string(inputData))
	if filterErr != 0 {
		t.Errorf("FilterLogic returned error: %v", filterErr)
	}
	testing_utils.LogStep("FilterLogic result: %s", result.ChunkData)
	if len(result.ChunkData) == 0 {
		t.Errorf("FilterLogic returned empty result")
	}

	// Read expected output
	expectedData, err := os.ReadFile("test_expected.csv")
	if err != nil {
		t.Fatalf("Failed to read expected output file: %v", err)
	}

	// Compare result with expected output
	assert.Equal(t, string(expectedData), result.ChunkData)

}
