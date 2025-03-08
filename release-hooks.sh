#!/bin/bash

update_all_versions() {
    local new_version=$1
    
    # Update pyproject.toml
    sed -i '' "s/version = \"[0-9.]*\"/version = \"$new_version\"/" ../pyproject.toml
    git add ../pyproject.toml
    
    # You could update other files that need version info here too
}

case $1 in
    "pre-release")
        update_all_versions $2
        ;;
esac