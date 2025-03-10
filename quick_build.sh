#!/bin/bash

cargo build --workspace --exclude pyclient $1
maturin develop $1

# if $1 is '--release', let's build that release
if [ "$1" = "--release" ]; then
    cargo release patch --no-publish --execute
fi