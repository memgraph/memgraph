---
name: memgraph-generate-docker-images
description: Build MAGE Docker images locally for testing. Use when the user wants to build a local MAGE image, test new query modules in Docker, create custom MAGE builds, or package MAGE with specific Memgraph versions.
---

# Building MAGE Docker Images Locally

This skill explains how to build MAGE Docker images locally for testing new query modules or custom configurations.

## Quick Start

From the repository root (`memgraph/`):

```bash
# Build with latest Memgraph release (simplest)
./tools/ci/mage-build/build-docker-image.sh

# Build with custom image tag
./tools/ci/mage-build/build-docker-image.sh --image-tag my-test

# Build with specific Memgraph package
./tools/ci/mage-build/build-docker-image.sh \
  --memgraph-url "https://download.memgraph.com/memgraph/v3.0.0/ubuntu-24.04/memgraph_3.0.0-1_amd64.deb" \
  --image-tag v3.0.0-test
```

## Script Location

```
tools/ci/mage-build/build-docker-image.sh
```

## Available Options

| Option | Description | Default |
|--------|-------------|---------|
| `--image-tag <tag>` | Docker image tag name | `custom` |
| `--memgraph-url <url>` | URL to Memgraph .deb package | Latest release |
| `--build-type <type>` | Build type: `Release` or `RelWithDebInfo` | `Release` |

## What the Script Does

1. **Downloads Memgraph package** - Either from the provided URL or fetches the latest release
2. **Launches mgbuild container** - Uses the toolchain container to build MAGE
3. **Builds MAGE modules** - Compiles C++, Rust, and packages Python modules
4. **Creates Docker image** - Builds `memgraph/memgraph-mage:<tag>`

## Build Types

### Release (Default)
Standard production build with optimizations.

```bash
./tools/ci/mage-build/build-docker-image.sh --build-type Release
```

### RelWithDebInfo
Debug build with symbols, GDB support, and heaptrack for profiling.

```bash
./tools/ci/mage-build/build-docker-image.sh --build-type RelWithDebInfo
```

## Examples

### Build and Test a New Query Module

After creating a new Python module (e.g., `mg_semantics.py`):

```bash
# 1. Build the Docker image
./tools/ci/mage-build/build-docker-image.sh --image-tag test-semantics

# 2. Run the container
docker run --rm -p 7687:7687 -p 7444:7444 memgraph/memgraph-mage:test-semantics

# 3. Connect and test your module
# In another terminal, run tests or connect with mgconsole
```

### Build with Specific Memgraph Version

```bash
# Use a specific version
./tools/ci/mage-build/build-docker-image.sh \
  --memgraph-url "https://download.memgraph.com/memgraph/v3.0.0/ubuntu-24.04/memgraph_3.0.0-1_amd64.deb" \
  --image-tag v3.0.0

# For ARM64 architecture
./tools/ci/mage-build/build-docker-image.sh \
  --memgraph-url "https://download.memgraph.com/memgraph/v3.0.0/ubuntu-24.04-aarch64/memgraph_3.0.0-1_arm64.deb" \
  --image-tag v3.0.0-arm
```

### Build Debug Image for Profiling

```bash
./tools/ci/mage-build/build-docker-image.sh \
  --build-type RelWithDebInfo \
  --image-tag debug

# Run with GDB support available
docker run --rm -it -p 7687:7687 memgraph/memgraph-mage:debug
```

## Running the Built Image

```bash
# Basic run
docker run --rm -p 7687:7687 -p 7444:7444 memgraph/memgraph-mage:<tag>

# With persistent data
docker run --rm -p 7687:7687 -p 7444:7444 \
  -v mg_data:/var/lib/memgraph \
  memgraph/memgraph-mage:<tag>

# With custom configuration
docker run --rm -p 7687:7687 -p 7444:7444 \
  memgraph/memgraph-mage:<tag> \
  --log-level=DEBUG \
  --query-modules-directory=/usr/lib/memgraph/query_modules
```

## Verifying the Build

After building, verify your modules are loaded:

```bash
# Connect to Memgraph
docker exec -it <container_id> mgconsole

# List all loaded modules
CALL mg.procedures() YIELD name RETURN name;

# Check specific module
CALL mg.procedures() YIELD name WHERE name STARTS WITH 'mg_semantics' RETURN name;
```

## Troubleshooting

### Build Fails with "mgbuild container not found"

The script uses `mgbuild.sh` which requires the mgbuild Docker image. It will be pulled automatically, but ensure Docker is running:

```bash
docker info
```

### Out of Disk Space

MAGE builds can be large. Clean up old images:

```bash
docker system prune -a
```

### Module Not Found After Build

Ensure your module is in the correct location (`mage/python/`) and has no syntax errors:

```bash
# Check Python syntax
python3 -m py_compile mage/python/your_module.py
```

### Permission Denied

Run the script from the repository root with appropriate permissions:

```bash
cd /path/to/memgraph
./tools/ci/mage-build/build-docker-image.sh
```

## Architecture Detection

The script automatically detects your system architecture:
- `x86_64` → builds `amd64` image
- `aarch64` → builds `arm64` image

To verify:
```bash
arch  # Shows your architecture
```

## Clean Up

After testing, remove the custom images:

```bash
# Remove specific image
docker rmi memgraph/memgraph-mage:<tag>

# Remove all MAGE images
docker rmi $(docker images memgraph/memgraph-mage -q)
```

## Related Files

| File | Purpose |
|------|---------|
| `tools/ci/mage-build/build-docker-image.sh` | Main build script |
| `tools/ci/mage-build/build.sh` | MAGE compilation script |
| `mage/Dockerfile.release` | Docker image definition |
| `release/package/mgbuild.sh` | Build toolchain manager |
| `mage/python/requirements.txt` | Python dependencies |

## Integration with Development Workflow

### Typical Development Cycle

1. **Make changes** to query modules in `mage/python/`
2. **Build image** with `build-docker-image.sh --image-tag dev`
3. **Run container** and test changes
4. **Run e2e tests** against the container
5. **Iterate** until tests pass

### Running E2E Tests Against Your Image

```bash
# Start your custom image
docker run --rm -d -p 7687:7687 --name mage-test memgraph/memgraph-mage:dev

# Run tests
cd mage/tests
source ../env/bin/activate  # or create venv with test requirements
python3 test_e2e -k your_module

# Stop container
docker stop mage-test
```
