#!/bin/bash

# set -x  # Print commands as they execute
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

# Load required modules
module load PrgEnv-llvm/1.0

# Set up C compiler for libffi-sys
export CC=$(which clang)
echo "Using C compiler: $CC ($(which $CC))"

# Find libclang using clang location
CLANG_PATH=$(which clang)
CLANG_DIR=$(dirname "$CLANG_PATH")
export LIBCLANG_PATH="$CLANG_DIR/../lib"
echo "Setting LIBCLANG_PATH to: $LIBCLANG_PATH"

# Verify libclang.so exists
if [ -f "$LIBCLANG_PATH/libclang.so" ]; then
    echo "Found libclang.so"
else
    echo "Warning: libclang.so not found in $LIBCLANG_PATH"
fi

echo -e "\nBuilding project..."
cd ..
pwd

# test if $1 is set
if [ -n "$1" ]; then
cargo clean
fi

cargo build --release || {
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
