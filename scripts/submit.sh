#!/bin/bash

set -e  # Exit on error
SCRIPT_DIR="$(dirname "$0")"
cd "$SCRIPT_DIR"

# First generate all test scripts
echo "Generating test scripts..."
./gen_script.sh

# Verify scripts were generated
echo -e "\nVerifying generated scripts:"
ls -l server_scale_*.sh client_scale_*.sh || {
    echo "Error: No test scripts were generated!"
    exit 1
}

# Make sure all scripts are executable
chmod +x server_scale_*.sh client_scale_*.sh

# Function to submit a batch of tests
submit_batch() {
    local test_type=$1
    local pattern=$2
    echo -e "\nSubmitting $test_type tests..."
    
    for script in ${pattern}*.sh; do
        if [ -f "$script" ]; then
            echo "Submitting $script..."
            sbatch "$script"
            # Show the queue after each submission
            sleep 2
            squeue -u $USER
            echo "---"
        fi
    done
}

# Build the project first
echo -e "\nBuilding project..."
cd ..
cargo build || {
    echo "Error: Build failed!"
    exit 1
}

cd "$SCRIPT_DIR"

# Submit server scaling tests
submit_batch "server scaling" "server_scale_"

# Submit client scaling tests
submit_batch "client scaling" "client_scale_"

echo -e "\nFinal queue status:"
squeue -u $USER

echo -e "\nAll jobs submitted. Check status with 'squeue -u $USER'"
echo "Results will be in:"
echo "- PDC_TMPDIR: $SCRATCH/data/bulki_store/conf/"
echo "- Slurm outputs: $SCRIPT_DIR/o*.bulki_test.out"
