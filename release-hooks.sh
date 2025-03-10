#!/bin/bash

update_all_versions() {
    # Extract version from the workspace Cargo.toml file
    local new_version=$(grep -A 1 "\[workspace.package\]" ../Cargo.toml | grep "version" | sed 's/version = "\(.*\)"/\1/')
    echo "Detected workspace version: $new_version"
    
    # Update pyproject.toml
    sed -i '' "s/version = \"[0-9.]*\"/version = \"$new_version\"/" ../pyproject.toml
    git add ../pyproject.toml
    
    # You could update other files that need version info here too
}

case $1 in
    "pre-release")
        update_all_versions
        ;;
esac