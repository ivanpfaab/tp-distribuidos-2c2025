package common

// GetYearFromPartition returns year string from partition number
// DEPRECATED: This function is no longer used with user-based partitioning.
// Kept for backward compatibility only.
func GetYearFromPartition(partition int) string {
	switch partition {
	case 0, 1:
		return "2024"
	case 2:
		return "2025"
	default:
		return "2024" // Fallback
	}
}

// GetYearSemesterFromPartition returns year and semester strings from partition number
// DEPRECATED: This function is no longer used with user-based partitioning.
// Kept for backward compatibility only.
func GetYearSemesterFromPartition(partition int) (string, string) {
	switch partition {
	case 0:
		return "2024", "1"
	case 1:
		return "2024", "2"
	case 2:
		return "2025", "1"
	default:
		return "2024", "1" // Fallback
	}
}

