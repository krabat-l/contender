#!/bin/bash

# Default parameters
DEFAULT_TPS=15000
DEFAULT_DURATION=20
DEFAULT_HTTP_URL="http://127.0.0.1:9945"
DEFAULT_WS_URL="ws://127.0.0.1:9946"
DEFAULT_PRIVATE_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# Get command line arguments
TPS=${1:-$DEFAULT_TPS}
DURATION=${2:-$DEFAULT_DURATION}
HTTP_URL=${3:-$DEFAULT_HTTP_URL}
WS_URL=${4:-$DEFAULT_WS_URL}
PRIVATE_KEY=${5:-$DEFAULT_PRIVATE_KEY}

# Log directory
LOG_DIR="contender_logs"
mkdir -p $LOG_DIR

# Function to get current timestamp
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Function to run a single command
run_command() {
    local cmd="$1"
    local log_file="$2"
    local timestamp=$(get_timestamp)

    echo "[$timestamp] Executing command: $cmd"
    echo "[$timestamp] Executing command: $cmd" >> "$log_file"

    eval "$cmd" >> "$log_file" 2>&1

    local exit_code=$?
    timestamp=$(get_timestamp)
    echo "[$timestamp] Command completed, exit code: $exit_code"
    echo "[$timestamp] Command completed, exit code: $exit_code" >> "$log_file"

    return $exit_code
}

# Counter
counter=1

while true; do
    print_separator "Starting Round $counter"

    # UniV2 setup
    print_separator "Starting UniV2 Setup"
    cmd1="./target/release/contender setup ./scenarios/uniV2.toml $HTTP_URL $WS_URL -p $PRIVATE_KEY --min-balance=\"1000\""
    run_command "$cmd1" "$LOG_DIR/univ2_setup.log"
    print_separator "Completed UniV2 Setup"

    # UniV2 spam
    print_separator "Starting UniV2 Spam"
    cmd2="./target/release/contender spam ./scenarios/uniV2.toml $HTTP_URL $WS_URL --tps $TPS -d $DURATION -p $PRIVATE_KEY --min-balance=\"10\""
    run_command "$cmd2" "$LOG_DIR/univ2_spam.log"
    print_separator "Completed UniV2 Spam"

    # Pre-ERC20 setup
    print_separator "Starting Pre-ERC20 Setup"
    cmd3="./target/release/contender setup ./scenarios/pre-erc20.toml $HTTP_URL $WS_URL -p $PRIVATE_KEY"
    run_command "$cmd3" "$LOG_DIR/pre_erc20_setup.log"
    print_separator "Completed Pre-ERC20 Setup"

    # Pre-ERC20 spam
    print_separator "Starting Pre-ERC20 Spam"
    cmd4="./target/release/contender spam ./scenarios/pre-erc20.toml $HTTP_URL $WS_URL --tps $TPS -d 1 -p $PRIVATE_KEY --min-balance=\"1\""
    run_command "$cmd4" "$LOG_DIR/pre_erc20_spam.log"
    print_separator "Completed Pre-ERC20 Spam"

    # Do-ERC20 spam
    print_separator "Starting Do-ERC20 Spam"
    cmd5="./target/release/contender spam ./scenarios/do-erc20.toml $HTTP_URL $WS_URL --tps $TPS -d $DURATION -p $PRIVATE_KEY --min-balance=\"10\""
    run_command "$cmd5" "$LOG_DIR/do_erc20_spam.log"
    print_separator "Completed Do-ERC20 Spam"

    print_separator "Round $counter completed"

    ((counter++))

done