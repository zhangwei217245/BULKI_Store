#!/bin/bash

# Get the current version from root Cargo.toml
CURRENT_VERSION=$(grep 'version =' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/')

# Split the version into major, minor, patch
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Increment patch version
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="$MAJOR.$MINOR.$NEW_PATCH"

# Update the version in root Cargo.toml
sed -i '' "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" Cargo.toml

# Update the version in pyproject.toml as well
sed -i '' "s/version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml

# Add the modified files to the commit
git add Cargo.toml pyproject.toml

echo "Version bumped from $CURRENT_VERSION to $NEW_VERSION"