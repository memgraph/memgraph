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
| `--memgraph-url <url>` | URL to Memgraph `.deb` package | Latest release (auto-fetched via `tools/ci/get_latest_tag.sh`) |
| `--build-type <type>` | Build type: `Release` or `RelWithDebInfo` | `Release` |
| `--cugraph` | Enable cuGraph GPU-accelerated module build | Disabled |

## What the Script Does

1. **Downloads Memgraph package** — Either from the provided `--memgraph-url` or fetches the latest release tag and constructs the download URL
2. **Launches mgbuild container** — Runs `release/package/mgbuild.sh run` with the appropriate toolchain (`v7`), OS (`ubuntu-24.04`), arch, and build type
3. **Builds MAGE modules** — Runs `release/package/mgbuild.sh build-mage` which compiles C++, Rust, and packages Python modules
4. **Builds heaptrack** (RelWithDebInfo only) — Builds and copies heaptrack into the image for memory profiling
5. **Packages Docker image** — Runs `release/package/mgbuild.sh package-mage-docker` to build `memgraph/memgraph-mage:<tag>` using `mage/Dockerfile.release`
6. **Cleans up** — Removes temporary files (`mage/memgraph.deb`, `mage/mage.tar.gz`, `mage/openssl/`)

## Architecture Detection

The script automatically detects system architecture from `arch`:
- `x86_64` → builds `amd64` image
- Anything else (e.g. `aarch64`) → builds `arm64` image

When no `--memgraph-url` is provided, the auto-constructed download URL uses the detected arch:
```
https://download.memgraph.com/memgraph/v${VERSION}/${OS_PATH}/memgraph_${VERSION}-1_${ARCH}64.deb
```

Where `OS_PATH` is `ubuntu-24.04` for Release or `ubuntu-24.04-relwithdebinfo` for RelWithDebInfo builds.

## Build Types

### Release (Default)
Standard production build with optimizations. Docker image uses the `prod` stage from `mage/Dockerfile.release`.

```bash
./tools/ci/mage-build/build-docker-image.sh --build-type Release
```

### RelWithDebInfo
Debug build with symbols, GDB support, and heaptrack for profiling. Docker image uses the `relwithdebinfo` stage which includes GDB, libc6-dbg, heaptrack, and the full MAGE source tree at `/mage/`.

```bash
./tools/ci/mage-build/build-docker-image.sh --build-type RelWithDebInfo
```

## Examples

### Build and Test a New Query Module

After creating or modifying a Python module (e.g., in `mage/python/`):

```bash
# 1. Build the Docker image
./tools/ci/mage-build/build-docker-image.sh --image-tag test-module

# 2. Run the container
docker run --rm -p 7687:7687 -p 7444:7444 memgraph/memgraph-mage:test-module

# 3. Connect and test your module (in another terminal)
docker exec -it <container_id> mgconsole
```

### Build with Specific Memgraph Version

```bash
# x86_64
./tools/ci/mage-build/build-docker-image.sh \
  --memgraph-url "https://download.memgraph.com/memgraph/v3.0.0/ubuntu-24.04/memgraph_3.0.0-1_amd64.deb" \
  --image-tag v3.0.0

# ARM64
./tools/ci/mage-build/build-docker-image.sh \
  --memgraph-url "https://download.memgraph.com/memgraph/v3.0.0/ubuntu-24.04/memgraph_3.0.0-1_arm64.deb" \
  --image-tag v3.0.0-arm
```

### Build Debug Image for Profiling

```bash
./tools/ci/mage-build/build-docker-image.sh \
  --build-type RelWithDebInfo \
  --image-tag debug

docker run --rm -it -p 7687:7687 memgraph/memgraph-mage:debug
```

### Build with cuGraph (GPU) Support

```bash
./tools/ci/mage-build/build-docker-image.sh \
  --cugraph \
  --image-tag gpu-test
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

# Check a specific module
CALL mg.procedures() YIELD name WHERE name STARTS WITH 'igraphalg' RETURN name;
```

## Troubleshooting

### Build Fails with Container Issues
The script uses `release/package/mgbuild.sh` which manages the mgbuild Docker container. Ensure Docker is running:
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
python3 -m py_compile mage/python/your_module.py
```

### Permission Denied
Run the script from the repository root:
```bash
cd /path/to/memgraph
./tools/ci/mage-build/build-docker-image.sh
```

## Clean Up

```bash
# Remove specific image
docker rmi memgraph/memgraph-mage:<tag>

# Remove all MAGE images
docker rmi $(docker images memgraph/memgraph-mage -q)
```

## Related Files

| File | Purpose |
|------|---------|
| `tools/ci/mage-build/build-docker-image.sh` | Main build orchestration script |
| `tools/ci/mage-build/build.sh` | MAGE compilation script (runs inside mgbuild container) |
| `tools/ci/mage-build/container-build.sh` | Container build helper |
| `tools/ci/mage-build/container-package-deb.sh` | DEB packaging helper |
| `tools/ci/mage-build/compress-query-modules.sh` | Query module compression |
| `tools/ci/get_latest_tag.sh` | Fetches latest Memgraph release tag |
| `mage/Dockerfile.release` | Docker image definition (`prod` and `relwithdebinfo` stages) |
| `release/package/mgbuild.sh` | Build toolchain container manager |
| `mage/python/requirements.txt` | Python dependencies |
| `mage/python/requirements-gpu.txt` | Python GPU dependencies |
