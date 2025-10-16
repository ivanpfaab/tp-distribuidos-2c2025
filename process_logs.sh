#!/bin/bash

# Process Existing Logs Script
# This script processes already available logs without capturing new ones

#First run docker logs --tail=40000 streaming-service > results.txt

RESULTS_FILE="results.txt"

echo "=== Processing Existing Logs ==="

# Check if results file exists
if [ ! -f "$RESULTS_FILE" ]; then
    echo "ERROR: $RESULTS_FILE not found!"
    echo "Please run: make docker-compose-logs SERVICE=streaming-service | tee results.txt"
    echo "Or capture logs first, then run this script."
    exit 1
fi

echo "Processing logs from $RESULTS_FILE..."
echo "Total lines: $(wc -l < "$RESULTS_FILE")"
echo ""

# Process CLI1
echo "Processing CLI1..."
grep "CLI1" "$RESULTS_FILE" > cli1_results.txt

# Process CLI2  
echo "Processing CLI2..."
grep "CLI2" "$RESULTS_FILE" > cli2_results.txt

# Check if we have data for both clients
if [ ! -s "cli1_results.txt" ]; then
    echo "WARNING: No CLI1 data found"
fi

if [ ! -s "cli2_results.txt" ]; then
    echo "WARNING: No CLI2 data found"
fi

# Process queries for CLI1
echo "Processing queries for CLI1..."
cat cli1_results.txt | grep "Q1" | cut -d'|' -f3,4 | cut -d',' -f1,6 | sort -t, -k2,2 > cli1_q1_results.txt
cat cli1_results.txt | grep "Q2" | cut -d'|' -f3,4 | cut -d',' -f1,2,7,6,4,5 | sort -t, -k5,5 -k1,1 -k2,2n > cli1_q2_results.txt
cat cli1_results.txt | grep "Q3" | cut -d'|' -f3,4 | cut -d',' -f1,2,6,4 | sort -t, -k1,1 -k2,2 -k3,3n > cli1_q3_results.txt
cat cli1_results.txt | grep "Q4" | cut -d'|' -f3,4 | cut -d',' -f2,3,4,6 | sort -t, -k1,1 -k3,3n > cli1_q4_results.txt

# Process queries for CLI2
echo "Processing queries for CLI2..."
cat cli2_results.txt | grep "Q1" | cut -d'|' -f3,4 | cut -d',' -f1,6 | sort -t, -k2,2 > cli2_q1_results.txt
cat cli2_results.txt | grep "Q2" | cut -d'|' -f3,4 | cut -d',' -f1,2,7,6,4,5 | sort -t, -k5,5 -k1,1 -k2,2n > cli2_q2_results.txt
cat cli2_results.txt | grep "Q3" | cut -d'|' -f3,4 | cut -d',' -f1,2,6,4 | sort -t, -k1,1 -k2,2 -k3,3n > cli2_q3_results.txt
cat cli2_results.txt | grep "Q4" | cut -d'|' -f3,4 | cut -d',' -f2,3,4,6 | sort -t, -k1,1 -k3,3n > cli2_q4_results.txt

echo ""
echo "=== Results Summary ==="
echo "CLI1 Results:"
echo "  Q1: $(wc -l < cli1_q1_results.txt 2>/dev/null || echo 0) lines"
echo "  Q2: $(wc -l < cli1_q2_results.txt 2>/dev/null || echo 0) lines"
echo "  Q3: $(wc -l < cli1_q3_results.txt 2>/dev/null || echo 0) lines"
echo "  Q4: $(wc -l < cli1_q4_results.txt 2>/dev/null || echo 0) lines"

echo ""
echo "CLI2 Results:"
echo "  Q1: $(wc -l < cli2_q1_results.txt 2>/dev/null || echo 0) lines"
echo "  Q2: $(wc -l < cli2_q2_results.txt 2>/dev/null || echo 0) lines"
echo "  Q3: $(wc -l < cli2_q3_results.txt 2>/dev/null || echo 0) lines"
echo "  Q4: $(wc -l < cli2_q4_results.txt 2>/dev/null || echo 0) lines"

echo ""
echo "=== Comparison ==="
for query in q1 q2 q3 q4; do
    cli1_file="cli1_${query}_results.txt"
    cli2_file="cli2_${query}_results.txt"
    
    if [ -f "$cli1_file" ] && [ -f "$cli2_file" ]; then
        if diff -q "$cli1_file" "$cli2_file" > /dev/null 2>&1; then
            echo "Q${query}: ✅ IDENTICAL"
        else
            echo "Q${query}: ❌ DIFFERENT"
            echo "  CLI1: $(wc -l < "$cli1_file") lines"
            echo "  CLI2: $(wc -l < "$cli2_file") lines"
        fi
    else
        echo "Q${query}: ⚠️  MISSING FILES"
    fi
done

echo ""
echo "=== Sample Results ==="
for client in cli1 cli2; do
    echo ""
    echo "--- $client ---"
    for query in q1 q2 q3 q4; do
        file="${client}_${query}_results.txt"
        if [ -f "$file" ] && [ -s "$file" ]; then
            echo "Query ${query^^}:"
            head -3 "$file" | sed 's/^/  /'
            echo "  ..."
        fi
    done
done

echo ""
echo "Done! Check the generated files:"
echo "  - cli1_results.txt, cli2_results.txt (client-specific logs)"
echo "  - cli1_q1_results.txt, cli1_q2_results.txt, etc. (query-specific results)"
echo "  - cli2_q1_results.txt, cli2_q2_results.txt, etc. (query-specific results)"
