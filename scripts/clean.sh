#!/bin/bash

SCRIPT_DIR="$(dirname "$0")"

# Cancel all running jobs
echo "Canceling all running jobs..."
squeue -u $USER -h -t running -o "%i" | xargs -r scancel

# Cancel all pending jobs
echo "Canceling all pending jobs..."
squeue -u $USER -h -t pending -o "%i" | xargs -r scancel

# Remove all generated test scripts
echo "Removing generated test scripts..."
rm -f $SCRIPT_DIR/server_scale_*.sh
rm -f $SCRIPT_DIR/client_scale_*.sh

# Clean PDC tmp directory
echo "Cleaning PDC_TMPDIR..."
rm -rf $SCRATCH/data/bulki_store/conf/*

# Remove slurm output files
echo "Cleaning slurm output files..."
rm -f $SCRIPT_DIR/o*.bulki_test.out

echo "Cleanup complete!"