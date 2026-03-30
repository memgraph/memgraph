---
name: mage-build
description: Build the MAGE (Memgraph Advanced Graph Extensions) Docker image using the CI build pipeline. Invoke when the user wants to build, rebuild, or customize a memgraph-mage Docker image.
---

You help the user build the MAGE Docker image using `tools/ci/mage-build/build-docker-image.sh`.

## Build Script

The entry point is:

```
./tools/ci/mage-build/build-docker-image.sh [OPTIONS]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--build-type` | `Release` | CMake build type (`Release` or `RelWithDebInfo`) |
| `--image-tag` | `custom` | Docker image tag for `memgraph/memgraph-mage:<tag>` |
| `--memgraph-url` | *(latest release)* | URL to a `.deb` Memgraph package to bake into the image |
| `--cugraph` | `false` | Enable cuGraph GPU-accelerated graph analytics |

### What the script does

1. **Fetches Memgraph package** — downloads from `--memgraph-url` or auto-resolves the latest release from `download.memgraph.com`
2. **Launches mgbuild container** — spins up the build environment via `release/package/mgbuild.sh`
3. **Builds MAGE** — compiles C++ and Rust query modules inside the container
4. **Builds heaptrack** *(RelWithDebInfo only)* — adds profiling support
5. **Packages Docker image** — creates `memgraph/memgraph-mage:<tag>`
6. **Cleans up** — removes temp artifacts (`mage/openssl`, `mage/memgraph.deb`, `mage/mage.tar.gz`)

## Workflow

When the user invokes this skill:

1. **Ask what they need** (if not clear from context):
   - Custom Memgraph `.deb` URL or use latest release?
   - Build type: `Release` (default) or `RelWithDebInfo` (with debug symbols + heaptrack)?
   - Custom image tag?
   - cuGraph support needed?

2. **Construct and run the command** from the repo root (`/home/josip/Memgraph-Repos/memgraph`):

   Minimal (latest Memgraph release, Release build):
   ```bash
   ./tools/ci/mage-build/build-docker-image.sh
   ```

   With a local/custom Memgraph build:
   ```bash
   ./tools/ci/mage-build/build-docker-image.sh \
     --memgraph-url "https://..." \
     --image-tag my-tag
   ```

   Debug build with heaptrack:
   ```bash
   ./tools/ci/mage-build/build-docker-image.sh \
     --build-type RelWithDebInfo \
     --image-tag debug
   ```

3. **Run the build** using Bash with a generous timeout (builds take several minutes). Stream output so the user can monitor progress.

4. **Report the result** — on success the image is `memgraph/memgraph-mage:<tag>`. Suggest next steps like `docker run` or running MAGE e2e tests.

## Important Notes

- The script must be run from the **repository root**.
- Architecture is auto-detected (`x86_64` -> `amd`, otherwise `arm`).
- The `memgraph-ref` is auto-set to the current git branch.
- The mgbuild container is started and stopped automatically; do not interfere with it during the build.
- If the build fails, check `release/package/mgbuild.sh` logs and ensure Docker is running.
