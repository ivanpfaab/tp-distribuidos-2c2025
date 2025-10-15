package main

import (
	"fmt"
	"time"

	"github.com/tp-distribuidos-2c2025/shared/queues"
)

// Semester represents a semester with year and semester number
type Semester struct {
	Year     int
	Semester int // 1 or 2
}

// GetSemesterFromDate calculates the semester from a given date
// January-June -> Semester 1, July-December -> Semester 2
// This is used for routing to the correct reduce worker
func GetSemesterFromDate(date time.Time) Semester {
	year := date.Year()
	month := int(date.Month())

	var semester int
	if month >= 1 && month <= 6 {
		semester = 1
	} else {
		semester = 2
	}

	return Semester{Year: year, Semester: semester}
}

// GetMonthFromDate extracts year and month from a date
func GetMonthFromDate(date time.Time) (int, int) {
	return date.Year(), int(date.Month())
}

// GetSemesterFromString parses a date string and returns the semester
func GetSemesterFromString(dateStr string) (Semester, error) {
	// Try different date formats
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.000Z",
	}

	for _, format := range formats {
		if date, err := time.Parse(format, dateStr); err == nil {
			return GetSemesterFromDate(date), nil
		}
	}

	// If no format matches, return error
	return Semester{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

// GetQueueNameForSemester returns the queue name for a specific semester (Query 2)
func GetQueueNameForSemester(semester Semester) string {
	return queues.GetQuery2ReduceQueueName(semester.Year, semester.Semester)
}

// GetAllSemesters returns all semesters from S2-2023 to S2-2025
func GetAllSemesters() []Semester {
	semesters := []Semester{}

	// S2-2023 to S2-2025
	for year := 2023; year <= 2025; year++ {
		// Add both semesters for each year
		semesters = append(semesters, Semester{Year: year, Semester: 1})
		semesters = append(semesters, Semester{Year: year, Semester: 2})
	}

	// Remove S1-2023 since we start from S2-2023
	semesters = semesters[1:]

	return semesters
}

// IsValidSemester checks if a semester is within our valid range
func IsValidSemester(semester Semester) bool {
	validSemesters := GetAllSemesters()
	for _, valid := range validSemesters {
		if valid.Year == semester.Year && valid.Semester == semester.Semester {
			return true
		}
	}
	return false
}

// SemesterToString converts a semester to a readable string
func (s Semester) String() string {
	return fmt.Sprintf("S%d-%d", s.Semester, s.Year)
}
