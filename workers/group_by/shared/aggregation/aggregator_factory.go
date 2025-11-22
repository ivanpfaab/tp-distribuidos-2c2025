package aggregation

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// NewAggregator creates an aggregator for the specified query type and partition
func NewAggregator(queryType int, partition int) (Aggregator, error) {
	switch queryType {
	case 2:
		return NewQuery2Aggregator()
	case 3:
		return NewQuery3Aggregator()
	case 4:
		return NewQuery4Aggregator()
	default:
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}
}

// NewAggregatorFromFile creates an aggregator by extracting partition from filename
func NewAggregatorFromFile(queryType int, filePath string) (Aggregator, error) {
	partition, err := common.ExtractPartitionFromFilename(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to extract partition from filename: %v", err)
	}

	return NewAggregator(queryType, partition)
}

