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
conda create --name your_env_name python=3.12
conda activate your_env_name
```
And then you can install maturin:
```bash
conda install conda-forge::maturin
```

### For pyenv or virtualenv users:
Please create a new virtual environment and install maturin:
```bash
python3 -m venv ~/.venv/your_env_name
source ~/.venv/your_env_name/bin/activate
pip install maturin
```

## Building commons and server

```bash
cargo build -p commons -p server --(debug|release)
```

## Building the client

```bash
maturin develop (-r)
```


## Running the server and client

Now, you can run the example in the jupyter notebook we provided.

To start the server and to observe the server debugging logs, you can open `BulkiStore_launcher.ipynb`.

To run the Python Demo, please open `Bulki_Store.ipynb`.

To install jupyter and configure its kernel with your conda or python environment, you can run:

```bash
jupyter kernelspec install --user --name your_env_name --display-name "BULKI Store (Python)"
```

After that, you can open `BulkiStore_launcher.ipynb` and `Bulki_Store.ipynb` in jupyter notebook.

Make sure you select the right kernel in jupyter notebook, it has to be the one matches with the env where you have the BULKI Store client installed.


## Release management

### Cargo Release

Make sure you have cargo-release installed:
```bash
cargo install cargo-release
```

## Build with Release
```bash
cargo clean; ./quick_build.sh --build-type=release|debug [--gen-release=patch|minor|major[,execute]]
```

### Syncing client/Cargo.toml version to pyproject.toml

We have prepared a script `release-hooks.sh` that will help you sync the version between Cargo.toml and pyproject.toml.

The script will be executed everytime you release a new version. 

### Creating a Release Manually

You can release a new patch version by running:
```bash
cargo release patch --no-publish
```

## Note on Perlmutter

### Module to load:

To use the python client, and to run the server properly, you need to load the llvm Program Environment:
```bash
module load PrgEnv-llvm
```

However, after doing this, your mpi4py from the NERSC default conda env will not work since it was compiled with mpich with PrgEnv-gnu. 

You need to reinstall mpi4py with mpich under PrgEnv-llvm.

```bash
MPICC="cc -shared" pip install --force-reinstall --no-cache-dir --no-binary=mpi4py mpi4py==3.1.5
```

Note that the python version should be the same as the one NERSC is using for the default mpi4py.


### Compatibility with MPI

On Perlmutter, the default cray-pe is PrgEnv-gnu. Therefore, we'd better compile our Rust code with PrgEnv-gnu. 

Here is the way to do it. 

First, you have to make sure that, if you are working with mpi4py, check its library:

```python
from mpi4py import MPI

print("MPI Library Version:", MPI.Get_library_version())
print("MPI Vendor:", MPI.get_vendor())
```

If you see the following, it means you are working with mpich with PrgEnv-gnu:

```
>>> print("MPI Library Version:", MPI.Get_library_version())
MPI Library Version: MPI VERSION    : CRAY MPICH version 8.1.30.8 (ANL base 3.4a2)
MPI BUILD INFO : Sat Jun 01  4:44 2024 (git hash 69863f7)

>>> print("MPI Vendor:", MPI.get_vendor())
MPI Vendor: ('MPICH', (3, 4, 0))
```

If you are not seeing the above, or seeing that your mpi4py is compiled with another MPI, say the llvm version, you can recompile you mpi4py with the mpich under PrgEnv-gnu. 

```bash
module load PrgEnv-gnu
module load cray-mpich
pip install --force-reinstall --no-cache-dir --no-binary=mpi4py mpi4py==3.1.5
```

Now, what you have to do is, if you have set MPICXX or CXX environment variables, you have to unset them:
```bash
unset MPICXX
unset CXX
```

And then, you can compile your Rust code with PrgEnv-gnu.

```bash
export CC=cc
export MPICC=cc
export MPI_LIB_DIR=$(dirname $(cc --cray-print-opts=libs | sed 's/-L//;s/ .*//'))
export MPI_INCLUDE_DIR=$(dirname $(cc --cray-print-opts=includes | sed 's/-I//;s/ .*//'))
cargo clean; cargo build -p commons -p server --release; maturin develop --release
```

