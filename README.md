

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

## Building commons and server

```bash
cargo build -p commons -p server --(debug|release)
```

## Building the client

```bash
maturin develop --manifest-path client/Cargo.toml
```


## Running the server

```bash
mpirun -np 4 ./target/debug/bulkistore_server
```


## Running the client

```bash
python

import bulkistore_client as bkc
bkc.init()

import numpy as np
arr1 = np.array([1, 2, 3], dtype=np.float64)
bkc.times_two(arr1)

```