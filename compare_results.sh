#!/bin/bash

# Comprehensive Results Comparison Script
# Compares client results against source of truth files

# Don't use set -e, we want to continue even if some comparisons fail
# Track failures instead

# Configuration
CLIENT_RESULTS_DIR="results"
SOURCE_OF_TRUTH_DIR="results_source_of_truth"
OUTPUT_DIR="comparison_results"
TEMP_DIR="temp_comparison"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create directories
mkdir -p "$OUTPUT_DIR" "$TEMP_DIR"

echo -e "${BLUE}=== COMPREHENSIVE RESULTS COMPARISON ===${NC}"
echo "Comparing client results against source of truth..."
echo ""

# Function to extract and clean client data
extract_client_data() {
    local client_id="$1"
    local query="$2"
    local output_file="$3"

    # Extract query data and remove client prefix
    # Use || true to prevent grep exit code 1 (no matches) from stopping the script
    grep "Q$query |" "$CLIENT_RESULTS_DIR/results_$client_id.txt" 2>/dev/null | \
    sed "s/^$client_id | Q$query | //" > "${output_file}.raw" || true
    
    # Remove ALL header lines that could appear anywhere in the file
    # Use a more comprehensive pattern that detects CSV headers by looking for:
    # 1. Lines with multiple field names separated by commas
    # 2. Lines that contain common field name patterns
    awk '
    BEGIN { 
        # Define header patterns for different query types
        q1_header = /transaction_id.*store_id.*payment_method_id/
        q2_header = /year.*month.*item_id.*quantity/
        q3_header = /year.*semester.*store_id.*total_final_amount/
        q4_header = /user_id.*store_id.*purchase_count.*rank/
    }
    {
        # Check if this line looks like a header
        is_header = 0
        
        # Q1 pattern: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        if ($0 ~ /transaction_id.*store_id.*payment_method_id/) is_header = 1
        
        # Q2 pattern: year,month,item_id,quantity,subtotal,category,item_name,coffee_category,price,is_seasonal
        if ($0 ~ /year.*month.*item_id.*quantity/) is_header = 1
        
        # Q3 pattern: year,semester,store_id,total_final_amount,count,store_name,street,postal_code,city,state,latitude,longitude
        if ($0 ~ /year.*semester.*store_id.*total_final_amount/) is_header = 1
        
        # Q4 pattern: user_id,store_id,purchase_count,rank,gender,birthdate,registered_at
        if ($0 ~ /user_id.*store_id.*purchase_count.*rank/) is_header = 1
        
        # Additional checks for common field names that indicate headers
        if ($0 ~ /^(transaction_id|year|user_id|item_id|store_name|purchase_count)[,]/) is_header = 1
        
        # Only print non-header lines
        if (!is_header) print $0
    }' "${output_file}.raw" > "$output_file"
    
    # Clean up
    rm "${output_file}.raw"
}

# Function to compare Q1 (exact match)
compare_q1() {
    local client_id="$1"
    local client_file="$TEMP_DIR/${client_id}_q1_transactions.csv"
    local source_file="$SOURCE_OF_TRUTH_DIR/q1_transactions.csv"

    echo "Comparing Q1 (Transactions) for $client_id..."

    # Check if files exist and have data
    if [ ! -f "$client_file" ] || [ ! -s "$client_file" ]; then
        echo -e "  Q1: ${YELLOW}⚠️ SKIPPED${NC} (No client data found)"
        return 0
    fi
    if [ ! -f "$source_file" ]; then
        echo -e "  Q1: ${YELLOW}⚠️ SKIPPED${NC} (Source of truth file missing)"
        return 0
    fi

    # Extract only relevant columns: transaction_id,final_amount
    # Client format: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
    # We need: transaction_id (col 1), final_amount (col 8)
    awk -F',' '{print $1","$8}' "$client_file" | sort > "${client_file}.extracted"
    tail -n +2 "$source_file" | sort > "${source_file}.sorted"
    
    if diff -q "${client_file}.extracted" "${source_file}.sorted" > /dev/null 2>&1; then
        echo -e "  Q1: ${GREEN}✅ PASS${NC} ($(wc -l < "${client_file}.extracted") data rows match)"
        # Keep only the extracted version, remove original
        mv "${client_file}.extracted" "$client_file"
        rm -f "${source_file}.sorted"
        return 0
    else
        echo -e "  Q1: ${RED}❌ FAIL${NC} (Differences found)"
        echo "    Client: $(wc -l < "${client_file}.extracted") data rows"
        echo "    Source: $(wc -l < "${source_file}.sorted") data rows"
        # Keep only the extracted version, remove original
        mv "${client_file}.extracted" "$client_file"
        rm -f "${source_file}.sorted"
        return 1
    fi
}

# Function to compare Q2 (category-based)
compare_q2() {
    local client_id="$1"
    local client_file="$TEMP_DIR/${client_id}_q2_categories.csv"

    echo "Comparing Q2 (Categories) for $client_id..."

    # Check if files exist and have data
    if [ ! -f "$client_file" ] || [ ! -s "$client_file" ]; then
        echo -e "  Q2 Best Selling (category=1): ${YELLOW}⚠️ SKIPPED${NC} (No client data found)"
        echo -e "  Q2 Most Profits (category=2): ${YELLOW}⚠️ SKIPPED${NC} (No client data found)"
        return 0
    fi

    # Split by category field (top classification indicator)
    # Category 1 = best selling (top by quantity)
    # Category 2 = most profits (top by revenue)
    
    # Extract category 1 (best selling - top by quantity)
    # Client format: year,month,item_id,quantity,subtotal,category,item_name,coffee_category,price,is_seasonal
    # We need: year_month_created_at (col1-col2), item_name (col7), sellings_qty (col4)
    awk -F',' '$6=="1" {printf "%s-%02d,%s,%d\n", $1, $2, $7, $4}' "$client_file" | sort > "$TEMP_DIR/${client_id}_q2_best_selling.csv"
    
    # Extract category 2 (most profits - top by revenue)
    # We need: year_month_created_at (col1-col2), item_name (col7), profit_sum (col5) - treat as integer
    awk -F',' '$6=="2" {printf "%s-%02d,%s,%d\n", $1, $2, $7, int($5)}' "$client_file" | sort > "$TEMP_DIR/${client_id}_q2_most_profits.csv"
    
    # Compare category 1 (top by quantity) against best_selling
    local best_selling_source="$SOURCE_OF_TRUTH_DIR/q2_best_selling.csv"
    tail -n +2 "$best_selling_source" | sort > "${best_selling_source}.sorted"
    
    if diff -q "$TEMP_DIR/${client_id}_q2_best_selling.csv" "${best_selling_source}.sorted" > /dev/null 2>&1; then
        echo -e "  Q2 Best Selling (category=1): ${GREEN}✅ PASS${NC} ($(wc -l < "$TEMP_DIR/${client_id}_q2_best_selling.csv") rows)"
    else
        echo -e "  Q2 Best Selling (category=1): ${RED}❌ FAIL${NC}"
        echo "    Client: $(wc -l < "$TEMP_DIR/${client_id}_q2_best_selling.csv") rows"
        echo "    Source: $(tail -n +2 "$best_selling_source" | wc -l) rows"
    fi
    
    # Compare category 2 (top by revenue) against most_profits
    local profits_source="$SOURCE_OF_TRUTH_DIR/q2_most_profits.csv"
    tail -n +2 "$profits_source" | awk -F',' '{printf "%s,%s,%d\n", $1, $2, int($3)}' | sort > "${profits_source}.sorted"
    
    if diff -q "$TEMP_DIR/${client_id}_q2_most_profits.csv" "${profits_source}.sorted" > /dev/null 2>&1; then
        echo -e "  Q2 Most Profits (category=2): ${GREEN}✅ PASS${NC} ($(wc -l < "$TEMP_DIR/${client_id}_q2_most_profits.csv") rows)"
    else
        echo -e "  Q2 Most Profits (category=2): ${RED}❌ FAIL${NC}"
        echo "    Client: $(wc -l < "$TEMP_DIR/${client_id}_q2_most_profits.csv") rows"
        echo "    Source: $(tail -n +2 "$profits_source" | wc -l) rows"
    fi
    
    # Clean up: remove original categories file and temporary sorted files
    rm -f "$client_file" "${best_selling_source}.sorted" "${profits_source}.sorted"
}

# Function to compare Q3 (with year-semester transformation)
compare_q3() {
    local client_id="$1"
    local client_file="$TEMP_DIR/${client_id}_q3_tpv.csv"
    local source_file="$SOURCE_OF_TRUTH_DIR/q3_tpv.csv"

    echo "Comparing Q3 (TPV) for $client_id..."

    # Check if files exist and have data
    if [ ! -f "$client_file" ] || [ ! -s "$client_file" ]; then
        echo -e "  Q3: ${YELLOW}⚠️ SKIPPED${NC} (No client data found)"
        return 0
    fi
    if [ ! -f "$source_file" ]; then
        echo -e "  Q3: ${YELLOW}⚠️ SKIPPED${NC} (Source of truth file missing)"
        return 0
    fi

    # Transform year,semester to year-H{semester} format
    # Client format: year,semester,store_id,total_final_amount,count,store_name,street,postal_code,city,state,latitude,longitude
    # We need: year_half_created_at (col1-col2), store_name (col6), tpv (col4) - treat as integer
    awk -F',' '{printf "%s-H%d,%s,%d\n", $1, $2, $6, int($4)}' "$client_file" | sort > "${client_file}.transformed"
    
    # Sort source file (skip header) and treat TPV as integer
    tail -n +2 "$source_file" | awk -F',' '{printf "%s,%s,%d\n", $1, $2, int($3)}' | sort > "${source_file}.sorted"
    
    if diff -q "${client_file}.transformed" "${source_file}.sorted" > /dev/null 2>&1; then
        echo -e "  Q3: ${GREEN}✅ PASS${NC} ($(wc -l < "${client_file}.transformed") rows match)"
        # Keep only the transformed version, remove original
        mv "${client_file}.transformed" "$client_file"
        rm -f "${source_file}.sorted"
        return 0
    else
        echo -e "  Q3: ${RED}❌ FAIL${NC} (Differences found)"
        echo "    Client: $(wc -l < "${client_file}.transformed") rows"
        echo "    Source: $(wc -l < "${source_file}.sorted") rows"
        # Keep only the transformed version, remove original
        mv "${client_file}.transformed" "$client_file"
        rm -f "${source_file}.sorted"
        return 1
    fi
}

# Function to compare Q4 (subset comparison)
compare_q4() {
    local client_id="$1"
    local client_file="$TEMP_DIR/${client_id}_q4_most_purchases.csv"
    local source_file="$SOURCE_OF_TRUTH_DIR/q4_most_purchases.csv"

    echo "Comparing Q4 (Most Purchases) for $client_id..."

    # Check if files exist and have data
    if [ ! -f "$client_file" ] || [ ! -s "$client_file" ]; then
        echo -e "  Q4: ${YELLOW}⚠️ SKIPPED${NC} (No client data found)"
        return 0
    fi
    if [ ! -f "$source_file" ]; then
        echo -e "  Q4: ${YELLOW}⚠️ SKIPPED${NC} (Source of truth file missing)"
        return 0
    fi

    # Extract relevant columns: store_id,birthdate,purchases_qty
    # Client format: user_id,store_id,purchase_count,rank,gender,birthdate,registered_at
    # We need: store_id (col2), birthdate (col6), purchases_qty (col3)
    awk -F',' '{print $2","$6","$3}' "$client_file" | sort > "${client_file}.extracted"
    
    # Sort source file (skip header)
    tail -n +2 "$source_file" | sort > "${source_file}.sorted"
    
    # Check how many client results are found in source of truth
    local found_count=$(comm -12 "${client_file}.extracted" "${source_file}.sorted" | wc -l)
    local client_count=$(wc -l < "${client_file}.extracted")
    local source_count=$(tail -n +2 "$source_file" | wc -l)
    
    local coverage_percent=$((found_count * 100 / client_count))
    
    if [ "$found_count" -eq "$client_count" ]; then
        echo -e "  Q4: ${GREEN}✅ PASS${NC} (All $client_count client results found in source of truth)"
    elif [ "$coverage_percent" -ge 90 ]; then
        echo -e "  Q4: ${YELLOW}⚠️ PARTIAL${NC} ($found_count/$client_count client results found, ${coverage_percent}% coverage)"
    else
        echo -e "  Q4: ${RED}❌ FAIL${NC} (Only $found_count/$client_count client results found, ${coverage_percent}% coverage)"
    fi
    
    echo "    Client: $client_count rows"
    echo "    Source: $source_count rows"
    echo "    Found: $found_count rows"
    echo "    Coverage: ${coverage_percent}%"
    
    # Show missing rows if any
    local missing_count=$((client_count - found_count))
    if [ "$missing_count" -gt 0 ]; then
        echo "    Missing $missing_count rows:"
        comm -23 "${client_file}.extracted" "${source_file}.sorted" | while read -r line; do
            echo "      $line"
        done
    fi
    
    # Clean up: keep only the extracted version, remove original and temporary files
    mv "${client_file}.extracted" "$client_file"
    rm -f "${source_file}.sorted"
}


# Main comparison function
compare_client() {
    local client_id="$1"

    echo -e "${BLUE}=== Processing $client_id ===${NC}"

    # Extract data for all queries (using exact source of truth naming)
    extract_client_data "$client_id" "1" "$TEMP_DIR/${client_id}_q1_transactions.csv"
    extract_client_data "$client_id" "2" "$TEMP_DIR/${client_id}_q2_categories.csv"  # Will be split into best_selling and most_profits
    extract_client_data "$client_id" "3" "$TEMP_DIR/${client_id}_q3_tpv.csv"
    extract_client_data "$client_id" "4" "$TEMP_DIR/${client_id}_q4_most_purchases.csv"

    echo ""
    echo -e "${BLUE}=== Comparison Results for $client_id ===${NC}"

    # Compare each query - use || true to continue even if comparison fails
    compare_q1 "$client_id" || true
    compare_q2 "$client_id" || true
    compare_q3 "$client_id" || true
    compare_q4 "$client_id" || true

    echo ""
}

# Check if required files exist (warns but doesn't exit)
check_files() {
    local missing_files=()

    for client in CLI1 CLI2; do
        if [ ! -f "$CLIENT_RESULTS_DIR/results_$client.txt" ]; then
            missing_files+=("$CLIENT_RESULTS_DIR/results_$client.txt")
        fi
    done

    for query_file in q1_transactions.csv q2_best_selling.csv q2_most_profits.csv q3_tpv.csv q4_most_purchases.csv; do
        if [ ! -f "$SOURCE_OF_TRUTH_DIR/$query_file" ]; then
            missing_files+=("$SOURCE_OF_TRUTH_DIR/$query_file")
        fi
    done

    if [ ${#missing_files[@]} -gt 0 ]; then
        echo -e "${YELLOW}WARNING: Missing some files (will skip those comparisons):${NC}"
        for file in "${missing_files[@]}"; do
            echo "  - $file"
        done
        echo ""
    fi
}

# Main execution
main() {
    echo "Checking required files..."
    check_files

    echo "Starting comparison..."
    echo ""

    # Compare both clients - continue even if one fails
    if [ -f "$CLIENT_RESULTS_DIR/results_CLI1.txt" ]; then
        compare_client "CLI1" || true
    else
        echo -e "${YELLOW}Skipping CLI1 - results file not found${NC}"
        echo ""
    fi

    if [ -f "$CLIENT_RESULTS_DIR/results_CLI2.txt" ]; then
        compare_client "CLI2" || true
    else
        echo -e "${YELLOW}Skipping CLI2 - results file not found${NC}"
        echo ""
    fi
    
    echo -e "${BLUE}=== SUMMARY ===${NC}"
    echo "Comparison completed!"
    echo "Detailed results saved in: $TEMP_DIR/"
    echo "Use 'ls -la $TEMP_DIR/' to see extracted data files"
    
    # Cleanup option
    echo ""
    read -p "Clean up temporary files? (y/N): " cleanup
    if [[ $cleanup =~ ^[Yy]$ ]]; then
        rm -rf "$TEMP_DIR"
        echo "Temporary files cleaned up."
    else
        echo "Temporary files preserved in: $TEMP_DIR/"
    fi
}

# Run main function
main "$@"
