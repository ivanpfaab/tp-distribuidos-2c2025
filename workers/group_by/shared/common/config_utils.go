package common

// GetNumWorkersForQuery returns the default number of workers for a specific query type
func GetNumWorkersForQuery(queryType int) int {
	switch queryType {
	case 2:
		return 3
	case 3:
		return 3
	case 4:
		return 3 // configurable
	default:
		return 1
	}
}
