#!/bin/bash

# Script to check volume sizes and client data in worker containers

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=== Checking volume sizes and client data ==="
echo ""

# Track containers with non-empty data folders
declare -a dirty_containers
declare -a dirty_details

for container in $(docker compose ps --format "{{.Name}}" 2>/dev/null); do
    # Check if container has /app/worker-data (data folder)
    has_data_folder=$(docker exec "$container" test -d /app/worker-data 2>/dev/null && echo "yes" || echo "no")

    if [ "$has_data_folder" = "yes" ]; then
        size=$(docker exec "$container" du -sh /app/worker-data 2>/dev/null | cut -f1)
        cli1_count=$(docker exec "$container" find /app/worker-data -type f -name "*CLI1*" 2>/dev/null | wc -l)
        cli2_count=$(docker exec "$container" find /app/worker-data -type f -name "*CLI2*" 2>/dev/null | wc -l)
        total_files=$(docker exec "$container" find /app/worker-data -type f 2>/dev/null | wc -l)

        echo "📁 $container"
        echo "   Size: $size | Total files: $total_files | CLI1: $cli1_count | CLI2: $cli2_count"

        # Track if this container has client-specific files
        if [ "$cli1_count" -gt 0 ] || [ "$cli2_count" -gt 0 ]; then
            dirty_containers+=("$container")
            # Collect file details for summary
            files_info=$(docker exec "$container" find /app/worker-data -type f \( -name "*CLI1*" -o -name "*CLI2*" \) -exec du -h {} \; 2>/dev/null | awk '{print "      " $2 " (" $1 ")"}')
            dirty_details+=("$container|$files_info")
        fi

        # Show CLI1 files if any exist
        if [ "$cli1_count" -gt 0 ]; then
            echo "   CLI1 files:"
            docker exec "$container" find /app/worker-data -type f -name "*CLI1*" 2>/dev/null | while read f; do
                fsize=$(docker exec "$container" du -h "$f" 2>/dev/null | cut -f1)
                echo "     - $f ($fsize)"
            done
        fi

        # Show CLI2 files if any exist
        if [ "$cli2_count" -gt 0 ]; then
            echo "   CLI2 files:"
            docker exec "$container" find /app/worker-data -type f -name "*CLI2*" 2>/dev/null | while read f; do
                fsize=$(docker exec "$container" du -h "$f" 2>/dev/null | cut -f1)
                echo "     - $f ($fsize)"
            done
        fi
        echo ""
    else
        # No data folder - show total container size
        total_size=$(docker exec "$container" du -sh /app 2>/dev/null | cut -f1)
        if [ -n "$total_size" ]; then
            echo "📦 $container [NO DATA FOLDER]"
            echo "   Total /app size: $total_size"
            echo ""
        fi
    fi
done

echo "=== Legend ==="
echo "📁 = Has /app/worker-data folder"
echo "📦 = No data folder (stateless or different structure)"
echo ""

echo "=== FINAL RESULT ==="
if [ ${#dirty_containers[@]} -eq 0 ]; then
    echo -e "${GREEN}✅ ALL CLEAN: No client-specific files found in any worker data folder${NC}"
else
    echo -e "${RED}⚠️  WARNING: ${#dirty_containers[@]} container(s) have client-specific files:${NC}"
    echo ""
    for detail in "${dirty_details[@]}"; do
        container_name=$(echo "$detail" | cut -d'|' -f1)
        files=$(echo "$detail" | cut -d'|' -f2-)
        echo -e "${YELLOW}   ❌ $container_name${NC}"
        echo "$files"
    done
fi
