#!/bin/bash -l

#SBATCH -q debug
#SBATCH -N 8  # Fixed 8 nodes for all tests
#SBATCH -t 0:30:00
#SBATCH -C cpu
#SBATCH -J |JOB_NAME|
#SBATCH -A m2621
#SBATCH -o o%j.|JOB_NAME|.out
#SBATCH -e o%j.|JOB_NAME|.out

# Perlmutter CPU node configuration
PHYSICAL_CORES_PER_CPU=64  # Each CPU has 64 physical cores
THREADS_PER_CORE=2  # Each physical core supports 2 hyperthreads
LOGICAL_CORES_PER_CPU=$((PHYSICAL_CORES_PER_CPU * THREADS_PER_CORE))

# Calculate number of processes and threads
N_NODE=$SLURM_JOB_NUM_NODES
NUM_SERVER_PROC_PER_NODE=NSERVER_PER_NODE  # Will be replaced by gen_script
NUM_CLIENT_PROC_PER_NODE=NCLIENT_PER_NODE  # Will be replaced by gen_script

NSERVER=$((N_NODE * NUM_SERVER_PROC_PER_NODE))
NCLIENT=$((N_NODE * NUM_CLIENT_PROC_PER_NODE))

# Calculate threads per process
NUM_THREAD_PER_SERVER_PROC=$((LOGICAL_CORES_PER_CPU / NUM_SERVER_PROC_PER_NODE))
NUM_THREAD_PER_CLIENT_PROC=$((LOGICAL_CORES_PER_CPU / NUM_CLIENT_PROC_PER_NODE))
if [ $NUM_THREAD_PER_CLIENT_PROC -lt 4 ]; then
    NUM_THREAD_PER_CLIENT_PROC=4  # Ensure minimum 2 physical cores
fi

# Print configuration
echo "Configuration:"
echo "- Nodes: $N_NODE"
echo "- Servers per node: $NUM_SERVER_PROC_PER_NODE (total: $NSERVER)"
echo "- Clients per node: $NUM_CLIENT_PROC_PER_NODE (total: $NCLIENT)"
echo "- Threads per server: $NUM_THREAD_PER_SERVER_PROC"
echo "- Threads per client: $NUM_THREAD_PER_CLIENT_PROC"

# Load required modules
module load PrgEnv-llvm/1.0

# Clean up and create PDC tmp directory
export PDC_TMPDIR=$SCRATCH/data/bulki_store/conf
export PDC_TMPDIR=${PDC_TMPDIR}/${N_NODE}_s${NSERVER}_c${NCLIENT}_st${NUM_THREAD_PER_SERVER_PROC}_ct${NUM_THREAD_PER_CLIENT_PROC}
rm -rf $PDC_TMPDIR/*
mkdir -p $PDC_TMPDIR

# Set paths
SERVER=$CFSHOME/perlmutter/source/BULKI_Store/target/release/bulkistore-server
CLIENT=$CFSHOME/perlmutter/source/BULKI_Store/target/release/bulkistore-client

# Clean up any existing ready files
rm -f /tmp/bulki_server_*_ready

# Start servers first and wait for them to be ready
echo "Starting $NSERVER servers..."
srun -N $N_NODE -n $NSERVER --ntasks-per-node=$NUM_SERVER_PROC_PER_NODE \
     -c $NUM_THREAD_PER_SERVER_PROC --cpu-bind=cores \
     $SERVER &

# Wait for all servers to create their ready files
MAX_WAIT=30
WAIT_COUNT=0
while [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    READY_COUNT=$(ls /tmp/bulki_server_*_ready 2>/dev/null | wc -l)
    if [ "$READY_COUNT" -eq "$NSERVER" ]; then
        echo "All servers are ready"
        break
    fi
    echo "Waiting for servers... ($READY_COUNT/$NSERVER ready)"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

# Additional wait to ensure network setup is complete
sleep 5

# Start all clients at once
echo "Starting $NCLIENT clients..."
srun -N $N_NODE -n $NCLIENT --ntasks-per-node=$NUM_CLIENT_PROC_PER_NODE \
     -c $NUM_THREAD_PER_CLIENT_PROC --cpu-bind=cores \
     $CLIENT &

wait
date
