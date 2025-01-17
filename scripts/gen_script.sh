#!/bin/bash

# set -x  # Print commands as they execute
set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Script directory: $SCRIPT_DIR"
cd "$SCRIPT_DIR"

TEMPLATE="template.sh"

# Check if template exists
if [ ! -f "$TEMPLATE" ]; then
    echo "Error: Template file $TEMPLATE not found!"
    ls -l
    exit 1
fi

# Perlmutter configuration
PHYSICAL_CORES_PER_CPU=64
THREADS_PER_CORE=2
LOGICAL_CORES_PER_CPU=$((PHYSICAL_CORES_PER_CPU * THREADS_PER_CORE))

# Fixed number of clients per node for server scaling test
CLIENTS_PER_NODE=64

# Function to generate a test script
generate_script() {
    local output_file=$1
    local servers_per_node=$2
    local clients_per_node=$3
    
    echo "Generating $output_file with $servers_per_node servers/node and $clients_per_node clients/node"
    cp "$TEMPLATE" "$output_file"
    
    # Replace placeholders
    sed -i "s/NSERVER_PER_NODE/$servers_per_node/" "$output_file"
    sed -i "s/NCLIENT_PER_NODE/$clients_per_node/" "$output_file"
    
    chmod +x "$output_file"
    echo "Generated $output_file:"
    ls -l "$output_file"
}

# Clean up any existing test scripts
rm -f server_scale_*.sh client_scale_*.sh

echo "Generating test scripts..."
echo "Note: Thread counts will be calculated automatically:"
echo "- Server threads = $LOGICAL_CORES_PER_CPU/server_processes_per_node (using all logical cores)"
echo "- Client threads = max($LOGICAL_CORES_PER_CPU/client_processes_per_node, 4) (minimum 2 physical cores)"
echo ""

# Generate server scaling test scripts (fixed 64 clients per node = 512 total clients)
echo "Server scaling tests (fixed 512 total clients):"
for servers_per_node in 1 2 4 8; do
    server_threads=$((LOGICAL_CORES_PER_CPU / servers_per_node))
    client_threads=$((LOGICAL_CORES_PER_CPU / CLIENTS_PER_NODE))  # 64 clients per node
    if [ $client_threads -lt 4 ]; then
        client_threads=4  # Ensure minimum 2 physical cores
    fi
    output_file="server_scale_${servers_per_node}x${CLIENTS_PER_NODE}.sh"
    generate_script "$output_file" $servers_per_node $CLIENTS_PER_NODE
    echo "- ${servers_per_node} servers/node ($server_threads logical cores each)"
    echo "  ${CLIENTS_PER_NODE} clients/node ($client_threads logical cores each)"
done

# Generate client scaling test scripts (fixed 8 servers per node = 64 total servers)
echo -e "\nClient scaling tests (fixed 64 total servers):"
for clients_per_node in 8 16 32 64; do
    server_threads=$((LOGICAL_CORES_PER_CPU / 8))  # 8 servers per node
    client_threads=$((LOGICAL_CORES_PER_CPU / clients_per_node))
    # Ensure minimum 4 threads (2 physical cores)
    if [ $client_threads -lt 4 ]; then
        client_threads=4
    fi
    output_file="client_scale_8x${clients_per_node}.sh"
    generate_script "$output_file" 8 $clients_per_node
    echo "- 8 servers/node ($server_threads logical cores each)"
    echo "  ${clients_per_node} clients/node ($client_threads logical cores each)"
done

echo "Generated scripts:"
ls -l server_scale_*.sh client_scale_*.sh
