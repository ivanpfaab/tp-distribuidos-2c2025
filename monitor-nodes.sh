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

CURRENT_LINE=0

move_to_line() {
    local line=$1
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
    if [ ! -f "$compose_file" ]; then
        echo "Error: docker-compose.yaml not found" >&2
        return 1
    fi
    
    grep "container_name:" "$compose_file" | sed 's/.*container_name: \(.*\)/\1/' | tr -d ' '
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

check_container_alive() {
    local container_name="$1"
    docker ps --format "{{.Names}}" 2>/dev/null | grep -q "^${container_name}$"
}

update_status() {
    for container in "${!node_category[@]}"; do
        if check_container_alive "$container"; then
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
    if [ "$status" == "alive" ]; then
        echo -n "${WHITE}"
    else
        echo -n "${RED}"
    fi
}

display_initial() {
    clear
    CURRENT_LINE=1
    
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
        local found=false
        
        for container in "${!node_category[@]}"; do
            if [ "${node_category[$container]}" == "$category" ]; then
                if [ "$found" == false ]; then
                    local cat_color=$(get_category_color "$category")
                    echo -e "${cat_color}━━━ $label ━━━${NC}"
                    CURRENT_LINE=$((CURRENT_LINE + 1))
                    found=true
                fi
                
                node_line_number["$container"]=$CURRENT_LINE
                
                local status="${node_status[$container]}"
                local history="${node_history[$container]}"
                local name_color=$(get_status_color "$status")
                
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
                printf "]\n"
                CURRENT_LINE=$((CURRENT_LINE + 1))
            fi
        done
        
        if [ "$found" == true ]; then
            echo ""
            CURRENT_LINE=$((CURRENT_LINE + 1))
        fi
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
    
    move_to_line 2
    clear_to_end_of_line
    printf "${GREEN}║${NC} ${WHITE}Node Status Monitor${NC} %-45s ${GREEN}║${NC}" "$timestamp"
    
    for container in "${!node_line_number[@]}"; do
        local line_num="${node_line_number[$container]}"
        local status="${node_status[$container]}"
        local history="${node_history[$container]}"
        local name_color=$(get_status_color "$status")
        
        local history_len=${#history}
        local history_display
        
        if [ $history_len -eq 0 ]; then
            history_display="."
        elif [ $history_len -le 50 ]; then
            history_display="$history"
        else
            history_display="${history: -50}"
        fi
        
        move_to_line "$line_num"
        clear_to_end_of_line
        printf "${name_color}%-40s${NC} [" "$container"
        print_colored_history "$history_display"
        printf "]"
    done
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
    display_initial
    
    while true; do
        update_status
        update_display
        sleep 1
    done
}

main "$@"
