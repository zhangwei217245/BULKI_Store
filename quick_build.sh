#!/bin/bash

# Initialize variables
BUILD_TYPE="debug"
RELEASE_TYPE=""
DRY_RUN=""

# Function to print usage
print_usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --build-type=<debug|release>    Set build type (default: debug)"
    echo "  --gen-release=<patch|minor|major>[,dry-run]    Generate a release"
    echo ""
    echo "Examples:"
    echo "  $0 --build-type=release         # Build in release mode"
    echo "  $0 --build-type=release --gen-release=patch          # Generate patch release"
    echo "  $0 --build-type=release --gen-release=minor,dry-run  # Test minor release process"
}

# Function to handle errors
handle_error() {
    echo "Error: $1"
    print_usage
    exit 1
}

# Parse arguments
for arg in "$@"; do
    case $arg in
        --build-type=*)
            BUILD_TYPE="${arg#*=}"
            if [ "$BUILD_TYPE" != "debug" ] && [ "$BUILD_TYPE" != "release" ]; then
                handle_error "Invalid build type. Must be 'debug' or 'release'"
            fi
            ;;
        --gen-release=*)
            IFS=',' read -ra RELEASE_ARGS <<< "${arg#*=}"
            RELEASE_TYPE="${RELEASE_ARGS[0]}"
            if [ "${RELEASE_ARGS[1]}" = "execute" ]; then
                RELEASE_EXECUTE="--execute"
            elif [ -n "${RELEASE_ARGS[1]}" ]; then
                handle_error "Invalid release option. Only 'execute' is supported"
            fi
            if [ "$RELEASE_TYPE" != "patch" ] && [ "$RELEASE_TYPE" != "minor" ] && [ "$RELEASE_TYPE" != "major" ]; then
                handle_error "Invalid release type. Must be 'patch', 'minor', or 'major'"
            fi
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            handle_error "Unknown argument: $arg"
            ;;
    esac
done

# Set build flags based on build type
BUILD_FLAGS=""
if [ "$BUILD_TYPE" = "release" ]; then
    BUILD_FLAGS="--release"
fi

# Build all crates except pyclient with the appropriate flags
echo "Building workspace with build type: $BUILD_TYPE"
cargo build --workspace --exclude pyclient $BUILD_FLAGS || handle_error "Workspace build failed"

# Build pyclient with maturin using the appropriate flags
echo "Building pyclient with maturin"
maturin develop $BUILD_FLAGS || handle_error "Maturin build failed"

# If release type was specified, perform the release
if [ -n "$RELEASE_TYPE" ]; then
    if [ "$BUILD_TYPE" != "release" ]; then
        handle_error "Release generation requires --build-type=release"
    fi
    
    echo "Performing $RELEASE_TYPE release..."
    if [ -z "$RELEASE_EXECUTE" ]; then
        echo "(Dry run mode - no version sync will be performed)"
    fi
    
    # Run cargo release with appropriate flags
    RELEASE_CMD="cargo release $RELEASE_TYPE --no-publish"
    if [ -n "$RELEASE_EXECUTE" ]; then
        RELEASE_CMD="$RELEASE_CMD --execute"
    fi
    
    echo "Running: $RELEASE_CMD"
    $RELEASE_CMD || handle_error "Release generation failed"
    
    # Version sync is handled by release-hooks.sh via cargo release hooks
    if [ -n "$RELEASE_EXECUTE" ]; then
        echo "Version sync will be performed by release-hooks.sh"
    fi
fi