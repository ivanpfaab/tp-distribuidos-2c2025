package main

import (
	"fmt"
	"time"
)

type Semester struct {
	Year     int
	Semester int
}

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

func (s Semester) String() string {
	return fmt.Sprintf("S%d-%d", s.Semester, s.Year)
}

func main() {
	date, err := time.Parse("2006-01-02 15:04:05", "2024-01-15 10:30:00")
	if err != nil {
		fmt.Printf("Error parsing date: %v\n", err)
		return
	}

	semester := GetSemesterFromDate(date)
	fmt.Printf("Date: %s\n", date.Format("2006-01-02 15:04:05"))
	fmt.Printf("Year: %d, Month: %d\n", date.Year(), int(date.Month()))
	fmt.Printf("Semester: %s\n", semester.String())
}

