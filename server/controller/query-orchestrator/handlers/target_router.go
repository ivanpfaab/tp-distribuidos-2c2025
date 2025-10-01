package handlers

import "fmt"

// TargetRouter handles the routing logic for determining which worker node
// should process a chunk based on QueryType and Step
type TargetRouter struct{}

// NewTargetRouter creates a new TargetRouter instance
func NewTargetRouter() *TargetRouter {
	return &TargetRouter{}
}

// DetermineTarget returns the target node based on QueryType and Step
func (tr *TargetRouter) DetermineTarget(queryType uint8, step int) string {
	// Routing logic based on QueryType and Step combinations

	fmt.Println("QueryType: ", queryType)
	fmt.Println("Step: ", step)

	switch queryType {
	case 1:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "filter"
		case 3:
			return "filter"
		}
	case 2:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "aggregator"
		case 3:
			return "groupby"
		case 4:
			return "filter"
		case 5:
			return "join"
		}
	case 3:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "filter"
		case 3:
			return "aggregator"
		case 4:
			return "groupby"
		case 5:
			return "join"
		}
	case 4:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "groupby"
		case 3:
			return "filter"
		case 4:
			return "join"
		}
	}

	// Default case - could be an error or default routing
	return "unknown"
}
