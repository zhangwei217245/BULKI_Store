# BULKI Store

A memory-safe Object Store implementation in Rust. 

Inspired by DAOS and PDC. 

Built for I/O Researchers. 

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


### Syncing client/Cargo.toml version to pyproject.toml

We have prepared a script `release-hooks.sh` that will help you sync the version between Cargo.toml and pyproject.toml.

The script will be executed everytime you release a new version. 

### Creating a Release Manually

You can release a new patch version by running:
```bash
cargo release patch --no-publish
```

## Build with Release
```bash
cargo clean; ./quick_build.sh --release
```
