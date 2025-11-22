package grouping

import (
	"fmt"
)

// NewGrouper creates a grouper for the specified query type and partition
func NewGrouper(queryType int, partition int) (RecordGrouper, error) {
	switch queryType {
	case 2:
		return NewQuery2Grouper(), nil
	case 3:
		return NewQuery3Grouper(), nil
	case 4:
		return NewQuery4Grouper(), nil
	default:
		return nil, fmt.Errorf("unsupported query type: %d", queryType)
	}
}

