package grouping

import (
	"fmt"

	"github.com/tp-distribuidos-2c2025/workers/group_by/shared/common"
)

// NewGrouper creates a grouper for the specified query type and partition
func NewGrouper(queryType int, partition int) (RecordGrouper, error) {
	switch queryType {
	case 2:
		year := common.GetYearFromPartition(partition)
		return NewQuery2Grouper(year), nil
	case 3:
		year, semester := common.GetYearSemesterFromPartition(partition)
		return NewQuery3Grouper(year, semester), nil
	case 4:
		return NewQuery4Grouper(), nil
	default:
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}
}

