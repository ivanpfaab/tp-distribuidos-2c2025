package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FileTypeInfo represents information about a file type group
type FileTypeInfo struct {
	TypeCode string
	Files    []string
}

// scanCSVFiles scans the data folder and returns all CSV files with their file IDs
func scanCSVFiles(dataFolder string) (map[string]string, error) {
	fileMap := make(map[string]string)
	var csvFiles []string

	// First, collect all CSV files
	err := filepath.Walk(dataFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process CSV files
		if !info.IsDir() && strings.HasSuffix(strings.ToLower(path), ".csv") {
			csvFiles = append(csvFiles, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort files for deterministic FileID assignment
	sort.Strings(csvFiles)

	// Group files by type for dynamic numbering
	fileTypeGroups := groupFilesByType(csvFiles)

	// Assign FileIDs based on file type/name patterns with dynamic numbering
	for _, path := range csvFiles {
		fileID := generateFileID(path, fileTypeGroups)
		fileMap[path] = fileID
		log.Printf("Found CSV file: %s (FileID: %s)", path, fileID)
	}

	return fileMap, nil
}

// groupFilesByType groups CSV files by their type for dynamic numbering
func groupFilesByType(csvFiles []string) map[string]*FileTypeInfo {
	typeGroups := make(map[string]*FileTypeInfo)

	for _, path := range csvFiles {
		fileType := determineFileType(path)

		if typeGroups[fileType] == nil {
			typeGroups[fileType] = &FileTypeInfo{
				TypeCode: getTypeCode(fileType),
				Files:    make([]string, 0),
			}
		}

		typeGroups[fileType].Files = append(typeGroups[fileType].Files, path)
	}

	return typeGroups
}

// determineFileType determines the file type based on path
func determineFileType(filePath string) string {
	dir := filepath.Dir(filePath)
	dirLower := strings.ToLower(dir)

	switch {
	case strings.Contains(dirLower, "stores"):
		return "stores"
	case strings.Contains(dirLower, "transactions"):
		return "transactions"
	case strings.Contains(dirLower, "transaction_items"):
		return "transaction_items"
	case strings.Contains(dirLower, "users"):
		return "users"
	case strings.Contains(dirLower, "menu_items"):
		return "menu_items"
	case strings.Contains(dirLower, "payment_methods"):
		return "payment_methods"
	case strings.Contains(dirLower, "vouchers"):
		return "vouchers"
	default:
		return "unknown"
	}
}

// getTypeCode returns the 2-character type code for a file type
func getTypeCode(fileType string) string {
	switch fileType {
	case "stores":
		return "ST"
	case "transactions":
		return "TR"
	case "transaction_items":
		return "TI"
	case "users":
		return "US"
	case "menu_items":
		return "MN"
	case "payment_methods":
		return "PM"
	case "vouchers":
		return "VC"
	default:
		return "XX"
	}
}

// generateFileID generates a deterministic FileID based on file path/name with dynamic numbering
// FileID Format: [TYPE][NUMBER] where:
//
//	TYPE: ST=Stores, TR=Transactions, TI=Transaction Items, US=Users, MN=Menu Items, PM=Payment Methods, VC=Vouchers, XX=Unknown
//	NUMBER: 01-99 assigned dynamically based on file order within each type
//
// Examples: ST01=stores.csv, TR01=transactions_202307.csv, TR02=transactions_202308.csv
func generateFileID(filePath string, fileTypeGroups map[string]*FileTypeInfo) string {
	fileType := determineFileType(filePath)
	typeInfo := fileTypeGroups[fileType]

	if typeInfo == nil {
		// Fallback for unknown types
		return fmt.Sprintf("XX%02d", 1)
	}

	// Find the position of this file within its type group
	fileNumber := 1
	for i, path := range typeInfo.Files {
		if path == filePath {
			fileNumber = i + 1
			break
		}
	}

	// Generate FileID with dynamic numbering
	return fmt.Sprintf("%s%02d", typeInfo.TypeCode, fileNumber)
}
