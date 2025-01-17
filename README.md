

## Compile on Perlmutter

Make sure you have protobuf installed, and you have to set the following environment variables:

```bash
export PROTOC=/path/to/protoc
```


If you don't have rust, please follow this instruction to install rust first:

https://www.rust-lang.org/tools/install

First, you need compile once when using the default module settings on Perlmutter:

```bash
module reset
cargo build
```

Then you will see libffi-sys fail and you need to load PrgEnv-llvm module:

```bash
module load PrgEnv-llvm/1.0
```

Finally, you can compile the project by running:

```bash
cargo llvm build
```