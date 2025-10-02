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
			return "streaming"
		}
	case 2:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "groupby"
		case 3:
			return "join"
		case 4:
			return "streaming"
		}
	case 3:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "groupby"
		case 3:
			return "join"
		case 4:
			return "streaming"
		}
	case 4:
		switch step {
		case 1:
			return "filter"
		case 2:
			return "groupby"
		case 3:
			return "join"
		case 4:
			return "streaming"
		}
	}

	// Default case - could be an error or default routing
	return "unknown"
}

// DetermineNextTarget returns the next target node based on QueryType and current Step
// This is used for processing replies and determining the next step in the pipeline
func (tr *TargetRouter) DetermineNextTarget(queryType uint8, currentStep int) string {
	fmt.Println("QueryType: ", queryType)
	fmt.Println("Current Step: ", currentStep)

	switch queryType {
	case 1:
		switch currentStep {
		case 1:
			return "streaming" // Filter -> Streaming
		case 2:
			return "unknown" // Already at final step
		}
	case 2:
		switch currentStep {
		case 1:
			return "groupby" // Filter -> GroupBy
		case 2:
			return "join" // GroupBy -> Join
		case 3:
			return "streaming" // Join -> Streaming
		case 4:
			return "unknown" // Already at final step
		}
	case 3:
		switch currentStep {
		case 1:
			return "groupby" // Filter -> GroupBy
		case 2:
			return "join" // GroupBy -> Join
		case 3:
			return "streaming" // Join -> Streaming
		case 4:
			return "unknown" // Already at final step
		}
	case 4:
		switch currentStep {
		case 1:
			return "groupby" // Filter -> GroupBy
		case 2:
			return "join" // GroupBy -> Join
		case 3:
			return "streaming" // Join -> Streaming
		case 4:
			return "unknown" // Already at final step
		}
	}

	// Default case - could be an error or default routing
	return "unknown"
}
