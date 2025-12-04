#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
NC='\033[0m'

CROSS="X"
DOT="."

declare -A node_status
declare -A node_history
declare -A node_category
declare -A node_line_number
declare -A node_column

CURRENT_LINE=0
SUPERVISOR_LEADER=""
SUPERVISOR_LEADER_LAST_CHECK=0
SUPERVISOR_LEADER_CACHE_DURATION=5

move_to_line() {
    local line=$1
    if [ $line -le 3 ]; then
        return
    fi
    echo -ne "\033[${line};1H"
}

clear_to_end_of_line() {
    echo -ne "\033[K"
}

print_colored_history() {
    local history="$1"
    local i
    local len=${#history}
    
    if [ $len -eq 0 ]; then
        echo -ne "${WHITE}.${NC}"
        return
    fi
    
    for ((i=0; i<$len; i++)); do
        local char="${history:$i:1}"
        if [ "$char" = "." ]; then
            echo -ne "${WHITE}.${NC}"
        elif [ "$char" = "X" ]; then
            echo -ne "${RED}X${NC}"
        else
            echo -ne "$char"
        fi
    done
}

extract_monitored_workers() {
    local compose_file="${1:-docker-compose.yaml}"
    if [ ! -f "$compose_file" ]; then
        echo "Error: docker-compose.yaml not found" >&2
        return 1
    fi
    
    local line=$(grep "MONITORED_WORKERS:" "$compose_file" | head -1)
    if [ -z "$line" ]; then
        return 1
    fi
    
    local monitored=$(echo "$line" | sed 's/.*MONITORED_WORKERS: "\([^"]*\)".*/\1/')
    if [ -z "$monitored" ]; then
        return 1
    fi
    
    echo "$monitored" | tr ',' '\n' | cut -d':' -f1 | grep -v '^$'
}

extract_all_containers() {
    local compose_file="${1:-docker-compose.yaml}"

    # Get all container names from docker-compose.yaml (not just running ones)
    # This ensures we monitor all nodes even if they're not alive when script starts
    if [ -f "$compose_file" ]; then
        # Parse services and their container_name (if set) from docker-compose.yaml
        # If container_name is set, use it; otherwise use service name
        awk '
            /^services:/ { in_services=1; next }
            in_services && /^[a-zA-Z]/ { in_services=0 }
            in_services && /^  [a-zA-Z0-9_-]+:/ {
                # New service found - print previous service if no container_name was found
                if (current_service != "" && container_name == "") {
                    print current_service
                }
                # Extract new service name
                current_service = $0
                sub(/:.*/, "", current_service)
                sub(/^  /, "", current_service)
                container_name = ""
            }
            in_services && /^    container_name:/ {
                # Found container_name - extract and print it
                container_name = $0
                sub(/.*container_name: */, "", container_name)
                gsub(/["'\'']/, "", container_name)  # Remove quotes
                print container_name
            }
            END {
                # Print last service if no container_name was found
                if (current_service != "" && container_name == "") {
                    print current_service
                }
            }
        ' "$compose_file" | sort
    else
        # Fallback to running containers if compose file not found
        docker ps --format "{{.Names}}" 2>/dev/null | sort
    fi
}

categorize_nodes() {
    local monitored_workers=$(extract_monitored_workers 2>/dev/null)
    
    while IFS= read -r container; do
        [ -z "$container" ] && continue
        
        if [ -n "$monitored_workers" ] && echo "$monitored_workers" | grep -q "^${container}$"; then
            node_category["$container"]="monitored"
        elif [[ "$container" =~ ^supervisor-[0-9]+$ ]]; then
            node_category["$container"]="supervisor"
        elif [[ "$container" =~ ^client-[0-9]+$ ]]; then
            node_category["$container"]="client"
        elif [[ "$container" == "rabbitmq-orchestrator" ]] || [[ "$container" == "chaos-monkey" ]] || [[ "$container" == "proxy-1" ]] || [[ "$container" == "test-runner" ]]; then
            node_category["$container"]="infrastructure"
        else
            node_category["$container"]="other"
        fi
    done < <(extract_all_containers)
}

get_supervisor_leader() {
    local supervisor_containers=()
    local running_containers
    running_containers=$(docker ps --format "{{.Names}}" 2>/dev/null)
    
    while IFS= read -r container; do
        if [[ "$container" =~ ^supervisor-[0-9]+$ ]]; then
            supervisor_containers+=("$container")
        fi
    done <<< "$running_containers"
    
    if [ ${#supervisor_containers[@]} -eq 0 ]; then
        echo ""
        return
    fi
    
    local leader=""
    local latest_timestamp=0
    
    for supervisor in "${supervisor_containers[@]}"; do
        local recent_logs
        recent_logs=$(docker logs --since 6s --timestamps "$supervisor" 2>&1 | grep "Broadcasting LEADER announcement" | tail -1)
        
        if [ -n "$recent_logs" ]; then
            local log_time_str
            log_time_str=$(echo "$recent_logs" | awk '{print $1}')
            
            if [ -n "$log_time_str" ]; then
                local timestamp
                timestamp=$(date -d "$log_time_str" +%s 2>/dev/null || echo 0)
                
                if [ $timestamp -gt $latest_timestamp ] && [ $timestamp -gt 0 ]; then
                    latest_timestamp=$timestamp
                    leader="$supervisor"
                fi
            fi
        fi
    done
    
    echo "$leader"
}

update_supervisor_leader() {
    local current_time=$(date +%s)
    local time_since_check=$((current_time - SUPERVISOR_LEADER_LAST_CHECK))
    
    if [ $time_since_check -ge $SUPERVISOR_LEADER_CACHE_DURATION ]; then
        SUPERVISOR_LEADER=$(get_supervisor_leader)
        SUPERVISOR_LEADER_LAST_CHECK=$current_time
    fi
}

update_status() {
    local running_containers
    running_containers=$(docker ps --format "{{.Names}}" 2>/dev/null)
    
    for container in "${!node_category[@]}"; do
        if echo "$running_containers" | grep -Fxq "$container"; then
            node_status["$container"]="alive"
        else
            node_status["$container"]="dead"
        fi
        
        if [ -z "${node_history[$container]}" ]; then
            node_history["$container"]=""
        fi
        
        if [ "${node_status[$container]}" == "alive" ]; then
            node_history["$container"]="${node_history[$container]}$DOT"
        else
            node_history["$container"]="${node_history[$container]}$CROSS"
        fi
        
        local max_history=100
        if [ ${#node_history[$container]} -gt $max_history ]; then
            node_history["$container"]="${node_history[$container]: -$max_history}"
        fi
    done
    
    update_supervisor_leader
}

get_category_color() {
    local category="$1"
    case "$category" in
        "monitored")
            echo -n "${CYAN}"
            ;;
        "supervisor")
            echo -n "${YELLOW}"
            ;;
        "client")
            echo -n "${MAGENTA}"
            ;;
        "infrastructure")
            echo -n "${BLUE}"
            ;;
        *)
            echo -n "${WHITE}"
            ;;
    esac
}

get_status_color() {
    local status="$1"
    local container="$2"
    
    if [ "$status" == "alive" ]; then
        if [ "$container" == "$SUPERVISOR_LEADER" ]; then
            echo -n "${YELLOW}"
        else
            echo -n "${WHITE}"
        fi
    else
        echo -n "${RED}"
    fi
}

get_terminal_width() {
    local width=$(tput cols 2>/dev/null || echo 120)
    echo $width
}

display_node_line() {
    local container="$1"
    local status="${node_status[$container]}"
    local history="${node_history[$container]}"
    local name_color=$(get_status_color "$status" "$container")
    
    local history_len=${#history}
    local history_display
    
    if [ $history_len -eq 0 ]; then
        history_display="."
    elif [ $history_len -le 50 ]; then
        history_display="$history"
    else
        history_display="${history: -50}"
    fi
    
    printf "${name_color}%-40s${NC} [" "$container"
    print_colored_history "$history_display"
    printf "]"
}

get_history_display_length() {
    local history="$1"
    local history_len=${#history}
    if [ $history_len -eq 0 ]; then
        echo 1
    elif [ $history_len -le 50 ]; then
        echo $history_len
    else
        echo 50
    fi
}

display_initial() {
    clear
    CURRENT_LINE=1
    
    local terminal_width=$(get_terminal_width)
    local col1_width=$((terminal_width / 2 - 2))
    local col2_start=$((terminal_width / 2 + 2))
    
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════════════════╗${NC}"
    CURRENT_LINE=$((CURRENT_LINE + 1))
    printf "${GREEN}║${NC} ${WHITE}Node Status Monitor${NC} %-45s ${GREEN}║${NC}\n" "$timestamp"
    CURRENT_LINE=$((CURRENT_LINE + 1))
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════════════════╝${NC}"
    CURRENT_LINE=$((CURRENT_LINE + 1))
    echo ""
    CURRENT_LINE=$((CURRENT_LINE + 1))
    
    local categories=("monitored" "supervisor" "infrastructure" "client" "other")
    local category_labels=("Monitored Workers" "Supervisors" "Infrastructure" "Clients" "Other")
    
    for i in "${!categories[@]}"; do
        local category="${categories[$i]}"
        local label="${category_labels[$i]}"
        
        local category_nodes=()
        for container in "${!node_category[@]}"; do
            if [ "${node_category[$container]}" == "$category" ]; then
                category_nodes+=("$container")
            fi
        done
        
        if [ ${#category_nodes[@]} -eq 0 ]; then
            continue
        fi
        
        local cat_color=$(get_category_color "$category")
        echo -e "${cat_color}━━━ $label ━━━${NC}"
        CURRENT_LINE=$((CURRENT_LINE + 1))
        
        local total_nodes=${#category_nodes[@]}
        local half=$(( (total_nodes + 1) / 2 ))
        
        local max_rows=$half
        local col_width=$((terminal_width / 2 - 5))
        
        for ((row=0; row<max_rows; row++)); do
            local idx1=$row
            local idx2=$((row + half))
            
            if [ $idx1 -lt $total_nodes ]; then
                local container1="${category_nodes[$idx1]}"
                node_line_number["$container1"]=$CURRENT_LINE
                node_column["$container1"]=1
                local hist1="${node_history[$container1]}"
                local hist1_len=$(get_history_display_length "$hist1")
                display_node_line "$container1"
                local used1=$((40 + 3 + hist1_len))
                if [ $used1 -lt $col_width ]; then
                    printf "%*s" $((col_width - used1)) ""
                fi
            else
                printf "%${col_width}s" ""
            fi
            
            printf "  "
            
            if [ $idx2 -lt $total_nodes ]; then
                local container2="${category_nodes[$idx2]}"
                node_line_number["$container2"]=$CURRENT_LINE
                node_column["$container2"]=2
                display_node_line "$container2"
            fi
            
            echo ""
            CURRENT_LINE=$((CURRENT_LINE + 1))
        done
        
        echo ""
        CURRENT_LINE=$((CURRENT_LINE + 1))
    done
    
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    CURRENT_LINE=$((CURRENT_LINE + 1))
    echo -e "${YELLOW}Legend:${NC} ${WHITE}${DOT}${NC} = Alive  ${RED}${CROSS}${NC} = Dead  (showing last 50 status checks, updates every 1s)"
    CURRENT_LINE=$((CURRENT_LINE + 1))
    echo -e "${YELLOW}Press Ctrl+C to exit${NC}"
    CURRENT_LINE=$((CURRENT_LINE + 1))
}

update_display() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    local terminal_width=$(get_terminal_width)
    local col_width=$((terminal_width / 2 - 5))
    local col2_start=$((col_width + 7))
    
    local right_col_nodes=()
    local left_col_nodes=()
    
    for container in "${!node_line_number[@]}"; do
        local line_num="${node_line_number[$container]}"
        if [ $line_num -le 3 ]; then
            continue
        fi
        local col="${node_column[$container]}"
        if [ "$col" == "2" ]; then
            right_col_nodes+=("$container")
        else
            left_col_nodes+=("$container")
        fi
    done
    
    for container in "${left_col_nodes[@]}"; do
        local line_num="${node_line_number[$container]}"
        local col="${node_column[$container]}"
        
        printf "\033[${line_num};1H"
        
        local status="${node_status[$container]}"
        local history="${node_history[$container]}"
        local name_color=$(get_status_color "$status" "$container")
        
        local history_len=${#history}
        local history_display
        
        if [ $history_len -eq 0 ]; then
            history_display="."
        elif [ $history_len -le 50 ]; then
            history_display="$history"
        else
            history_display="${history: -50}"
        fi
        
        printf "${name_color}%-40s${NC} [" "$container"
        print_colored_history "$history_display"
        printf "]"
        
        local hist_len=$(get_history_display_length "$history")
        local used=$((40 + 3 + hist_len))
        local remaining=$((col_width - used))
        if [ $remaining -gt 0 ]; then
            printf "%*s" $remaining ""
        fi
        printf "  "
        
        local total_used=$((used + remaining + 2))
        if [ $total_used -lt $terminal_width ]; then
            printf "%*s" $((terminal_width - total_used)) ""
        fi
    done
    
    for container in "${right_col_nodes[@]}"; do
        local line_num="${node_line_number[$container]}"
        local col="${node_column[$container]}"
        
        printf "\033[${line_num};${col2_start}H"
        
        local status="${node_status[$container]}"
        local history="${node_history[$container]}"
        local name_color=$(get_status_color "$status" "$container")
        
        local history_len=${#history}
        local history_display
        
        if [ $history_len -eq 0 ]; then
            history_display="."
        elif [ $history_len -le 50 ]; then
            history_display="$history"
        else
            history_display="${history: -50}"
        fi
        
        printf "${name_color}%-40s${NC} [" "$container"
        print_colored_history "$history_display"
        printf "]"
    done
    
    printf "\033[1;1H${GREEN}╔════════════════════════════════════════════════════════════════════════════╗${NC}"
    printf "\033[2;1H${GREEN}║${NC} ${WHITE}Node Status Monitor${NC} %-45s ${GREEN}║${NC}" "$timestamp"
    printf "\033[3;1H${GREEN}╚════════════════════════════════════════════════════════════════════════════╝${NC}"
    
    local min_line=999
    for container in "${!node_line_number[@]}"; do
        local line_num="${node_line_number[$container]}"
        if [ $line_num -lt $min_line ] && [ $line_num -gt 3 ]; then
            min_line=$line_num
        fi
    done
    
    if [ $min_line -lt 999 ]; then
        printf "\033[${min_line};1H"
    else
        printf "\033[4;1H"
    fi
    
    printf ""
}

main() {
    if ! command -v docker &> /dev/null; then
        echo "Error: docker command not found" >&2
        exit 1
    fi
    
    categorize_nodes
    
    if [ ${#node_category[@]} -eq 0 ]; then
        echo "Error: No containers found. Make sure docker-compose.yaml exists and contains container definitions." >&2
        exit 1
    fi
    
    trap 'echo -ne "\033[?25h"; clear; exit 0' INT TERM
    
    echo -ne "\033[?25l"
    
    update_status
    SUPERVISOR_LEADER=$(get_supervisor_leader)
    SUPERVISOR_LEADER_LAST_CHECK=$(date +%s)
    display_initial
    
    while true; do
        update_status
        update_display
        sleep 1
    done
}

main "$@"