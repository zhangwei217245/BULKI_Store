#!/bin/bash

set -x  # Print commands as they execute
set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Script directory: $SCRIPT_DIR"
cd "$SCRIPT_DIR"

# First generate all test scripts
echo "Generating test scripts..."
./gen_script.sh

echo "Listing current directory:"
ls -la

echo "Looking for generated scripts:"
find . -name "server_scale_*.sh" -o -name "client_scale_*.sh"

# Function to submit a batch of tests
submit_batch() {
    local test_type=$1
    local pattern=$2
    echo -e "\nSubmitting $test_type tests..."
    
    # Use find to get exact filenames
    find . -name "${pattern}_*.sh" | while read script; do
        echo "Found script: $script"
        echo "Submitting $script..."
        sbatch "$script"
        sleep 2
        squeue -u $USER
        echo "---"
    done
}

# Build the project first
echo -e "\nBuilding project..."
cd ..
pwd
cargo build || {
    echo "Error: Build failed!"
    exit 1
}

cd "$SCRIPT_DIR"

echo "Submitting server scaling tests..."
submit_batch "server scaling" "server_scale"

echo "Submitting client scaling tests..."
submit_batch "client scaling" "client_scale"

echo -e "\nFinal queue status:"
squeue -u $USER

echo -e "\nAll jobs submitted. Check status with 'squeue -u $USER'"
echo "Results will be in:"
echo "- PDC_TMPDIR: $SCRATCH/data/bulki_store/conf/"
echo "- Slurm outputs: $SCRIPT_DIR/o*.bulki_test.out"
