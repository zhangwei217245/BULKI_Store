# BULKI Store

## Prerequisites

MPICH 4.0+ is required.
protobuf 3.19.4+ is required.

## Compile on Perlmutter

Make sure you have protobuf installed, and you have to set the following environment variables:

```bash
export PROTOC=/path/to/protoc
```

If you don't have rust, please follow this instruction to install rust first:

https://www.rust-lang.org/tools/install


```bash
module load PrgEnv-llvm/1.0
export CC=$(which clang)
CLANG_DIR=$(dirname "$CC")
export LIBCLANG_PATH="$CLANG_DIR/../lib"
```

## Install maturin and python related dependencies


### For conda users:
If you are using conda, it's better if you create a new environment:
```bash
conda create --name bulkistore python=3.12
conda activate bulkistore
```
And then you can install maturin:
```bash
conda install conda-forge::maturin
```

### For pyenv or virtualenv users:
Please create a new virtual environment and install maturin:
```bash
python3 -m venv ~/.venv/bulkistore
source ~/.venv/bulkistore/bin/activate
pip install maturin
```

or if you are using conda, you can just do:
```bash
conda activate bulkistore
conda install conda-forge::maturin
```


## Building commons and server

```bash
cargo build -p commons -p server --(debug|release)
```

## Building the client

```bash
maturin develop (-r)
```


## Running the server

```bash
RUST_LOG=debug mpirun -np 4 target/debug/bulkistore-server
```


## Running the client

```bash
RUST_BACKTRACE=1 RUST_LOG=debug python testbkcobj3D.py
RUST_BACKTRACE=1 RUST_LOG=debug python testbkcobj5D.py 
```
