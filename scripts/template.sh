#!/bin/bash -l

#SBATCH -q regular
#SBATCH -N 8  # Fixed 8 nodes for all tests
#SBATCH -t 1:00:00
#SBATCH -C cpu
#SBATCH -J bulki_test
#SBATCH -A m2621
#SBATCH -o o%j.bulki_test.out
#SBATCH -e o%j.bulki_test.out

# Perlmutter CPU node configuration
PHYSICAL_CORES_PER_CPU=64  # Each CPU has 64 physical cores
THREADS_PER_CORE=2         # Hyperthreading: 2 logical cores per physical core
LOGICAL_CORES_PER_CPU=$((PHYSICAL_CORES_PER_CPU * THREADS_PER_CORE))

# Fixed number of nodes
N_NODE=8

# Server configuration (using CPU 1)
NUM_SERVER_PROC_PER_NODE=NSERVER_PER_NODE  # Will be replaced: 1,2,4,8 for scaling test
# Calculate threads per server process to use all logical cores of CPU 1
NUM_THREAD_PER_SERVER_PROC=$((LOGICAL_CORES_PER_CPU / NUM_SERVER_PROC_PER_NODE))

# Client configuration (using CPU 0)
NUM_CLIENT_PROC_PER_NODE=NCLIENT_PER_NODE  # Will be calculated based on available cores
# Calculate threads per client process
NUM_THREAD_PER_CLIENT_PROC=$((LOGICAL_CORES_PER_CPU / NUM_CLIENT_PROC_PER_NODE))

# Ensure minimum 4 threads per client process (2 physical cores)
if [ $NUM_THREAD_PER_CLIENT_PROC -lt 4 ]; then
    NUM_THREAD_PER_CLIENT_PROC=4
    # Adjust client processes if needed
    MAX_CLIENT_PROCS=$((LOGICAL_CORES_PER_CPU / 4))
    if [ $NUM_CLIENT_PROC_PER_NODE -gt $MAX_CLIENT_PROCS ]; then
        NUM_CLIENT_PROC_PER_NODE=$MAX_CLIENT_PROCS
        echo "Warning: Adjusted client processes to $NUM_CLIENT_PROC_PER_NODE to ensure minimum 4 threads (2 physical cores) per process"
    fi
fi

# Calculate total processes
NSERVER=$((NUM_SERVER_PROC_PER_NODE * N_NODE))
NCLIENT=$((NUM_CLIENT_PROC_PER_NODE * N_NODE))

# Directory setup
EXEC_DIR=/global/cfs/cdirs/m2621/wzhang5/perlmutter/source/BULKI_Store
EXECPATH=$EXEC_DIR/target/debug
SERVER=$EXECPATH/bulkistore-server
CLIENT=$EXECPATH/bulkistore-client

# Clean up and create PDC tmp directory
export PDC_TMPDIR=$SCRATCH/data/bulki_store/conf
export PDC_TMPDIR=${PDC_TMPDIR}/${N_NODE}_s${NSERVER}_c${NCLIENT}_st${NUM_THREAD_PER_SERVER_PROC}_ct${NUM_THREAD_PER_CLIENT_PROC}
rm -rf $PDC_TMPDIR/*
mkdir -p $PDC_TMPDIR

echo "Configuration:"
echo "- Servers: $NSERVER total ($NUM_SERVER_PROC_PER_NODE per node with $NUM_THREAD_PER_SERVER_PROC logical cores each)"
echo "- Clients: $NCLIENT total ($NUM_CLIENT_PROC_PER_NODE per node with $NUM_THREAD_PER_CLIENT_PROC logical cores each)"

# Start servers on CPU 1
srun -N $N_NODE -n $NSERVER --ntasks-per-node=$NUM_SERVER_PROC_PER_NODE \
     -c $NUM_THREAD_PER_SERVER_PROC --cpu_bind=map_cpu:64-127 $SERVER &
sleep 5

# Start clients on CPU 0
srun -N $N_NODE -n $NCLIENT --ntasks-per-node=$NUM_CLIENT_PROC_PER_NODE \
     -c $NUM_THREAD_PER_CLIENT_PROC --cpu_bind=map_cpu:0-63 $CLIENT

date
