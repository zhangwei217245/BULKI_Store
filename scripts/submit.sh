#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"

# First generate all test scripts
echo "Generating test scripts..."
$SCRIPT_DIR/gen_script.sh

# Function to submit a batch of tests
submit_batch() {
    local test_type=$1
    local pattern=$2
    echo "Submitting $test_type tests..."
    
    for script in $SCRIPT_DIR/${pattern}*.sh; do
        if [ -f "$script" ]; then
            echo "Submitting $script..."
            sbatch "$script"
            # Small delay to avoid overwhelming the scheduler
            sleep 1
        fi
    done
    echo ""
}

# Build the project first
echo "Building project..."
cd $SCRIPT_DIR/..
cargo build
echo ""

# Submit server scaling tests
submit_batch "server scaling" "server_scale_"

# Submit client scaling tests
submit_batch "client scaling" "client_scale_"

echo "All jobs submitted. Use 'squeue -u $USER' to check status."
echo "Results will be in the PDC_TMPDIR directory and slurm output files."
