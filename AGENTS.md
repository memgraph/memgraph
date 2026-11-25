# AGENTS.md

Guidance for coding agents working in the Memgraph repository.

## Repository overview

Memgraph is an in-memory graph database written in C++.

- `src/` — the database itself
- `query_modules/` — built-in query modules
- `tests/` — all test suites (see Tests below)
- `libs/` — vendored third-party dependencies, populated by setup scripts. **Do not edit.**
- `mgcxx/` — third-party text/vector search. **Do not edit.**
- `environment/` — OS dependency setup
- `release/` — packaging and the containerised builder
- `ADRs/` — architecture decision records
- `specs/` — design specifications
- `skills/` — agent skills that ship with the repo

## Build

**Do not run `cmake` directly.** Dependencies are resolved by Conan 2 and the CMake presets are generated. A plain `mkdir build && cmake .. && make` will not work.

Full guide: [Build Memgraph from source](https://memgraph.com/docs/getting-started/build-memgraph-from-source). Short version:

First time on a new machine — system packages, the toolchain, and Rust:

```
sudo ./environment/os/install_deps.sh install TOOLCHAIN_RUN_DEPS
sudo ./environment/os/install_deps.sh install MEMGRAPH_BUILD_DEPS
wget https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-v8/toolchain-v8-binaries-x86_64.tar.gz
sudo tar xzvfm toolchain-v8-binaries-x86_64.tar.gz -C /opt   # aarch64 archive on ARM
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
```

Memgraph compiles with **its own toolchain (v8)**, not the system compiler. It must be at `/opt/toolchain-v8`, or `MG_TOOLCHAIN_ROOT` must point at it. Python 3.10+ is required; set `MG_PYTHON` if the default `python3` is older.

First time in a fresh clone:

```
./init-dev      # git hooks + pre-commit (installs black 26.5.0, isort 5.12.*)
./init-test     # test environment: Neo4j 5.6.0 into libs/, Python venv from tests/requirements
```

Then:

```
./build.sh                       # Release (default)
./build.sh --build-type Debug    # also: RelWithDebInfo
./build.sh --dev                 # incremental, skips dependency checks — use while iterating
./build.sh --target <target>     # build one target
```

`build.sh` runs three stages: `conan install . --build=missing`, then `cmake --preset conan-release|conan-debug|conan-relwithdebinfo`, then `cmake --build build --preset <preset>`.

Other flags worth knowing: `--mage off|on|only`, `--no-python`, `--reserve-cores N`, `--compile-jobs N`, `--link-jobs N`, `--skip-os-deps`.

## Tests

Locally, after a build:

```
./build/memgraph --version     # check the build works at all
ctest -R unit -j$(nproc)       # unit tests
```

CI runs every suite through the containerised builder instead:

```
./release/package/mgbuild.sh --toolchain $TOOLCHAIN --os $OS --arch $ARCH \
  test-memgraph <suite>
```

Suites: `unit`, `gql-behave`, `query_modules_unit`, `query_modules_e2e`, `e2e-parallel`, `stress-plain`, `stress-ssl`, `durability`, `durability-large`.

<!-- TODO(kate): the local invocation for e2e without mgbuild — the docs only give the containerised one. -->

Rules:

- If you fix a bug or add code that should be tested, add a test.
- Suites covering Enterprise-gated behaviour need `MEMGRAPH_ENTERPRISE_LICENSE` and `MEMGRAPH_ORGANIZATION_NAME` in the environment. Without them Memgraph falls back to community mode and those paths are silently not exercised — the run passes without testing what you changed.

## Style and formatting

Formatting is enforced by pre-commit. Run it instead of reformatting by hand:

```
pre-commit run --all-files
```

- **C++** — `clang-format`, Google C++ Style Guide. Config is `.clang-format` at the root.
- **Python** — `black` 26.5.0 and `isort` 5.12.*, pinned by `init-dev`.
- A `.clang-tidy` config exists but its pre-commit hook is disabled. Do not assume clang-tidy runs on your change.
- `grappolo/`, `conan_recipes/` and `approval_tests/` are excluded from the formatting hooks. Leave their formatting alone.

## Conventions

- Pull requests target `master`.
- Read the relevant record in `ADRs/` before changing an area it covers. If a change makes a new architectural decision, add an ADR rather than arguing it in the PR description.
- Claim an issue by commenting on it before starting, so work is not duplicated.

## Commit messages

Conventional Commits. `CONTRIBUTING.md` does not say so, but `master` is consistent:

```
<type>(<scope>): <summary>
```

- **type** — one of `fix`, `feat`, `refactor`, `test`, `perf`, `build`, `docs`, `chore`.
- **scope** — optional but usual: the area touched, lowercase. Common ones are `query`, `storage`, `ci`, `utils`, `cmake`, `rpc`, `metrics`, `communication`. Omit it for repo-wide changes.
- **summary** — lowercase, no trailing full stop.
- The `(#1234)` suffix is appended automatically when the PR is squash-merged. **Do not type it yourself.**

Example: `fix(storage): add unique-constraint entries only for what a commit wrote`

## Skills in this repo

`skills/` holds portable agent skills for working on this codebase, following the Agent Skills Standard. They are for agents **working on Memgraph**, not for agents using it.

- `memgraph-storage-reviewer` — expert reviewer for the storage layer: MVCC, WAL, DDL, indices.

**If you are changing storage code, load it before reviewing your own work.** `skills/README.md` has the per-tool setup — for Claude Code, symlink into `.claude/skills/` and invoke `/memgraph-storage-reviewer`; there are equivalents for Cursor, Copilot, Windsurf and Aider.

## Do not

- Do not run plain `cmake`/`make`, or hand-edit the generated presets in `build/`.
- Do not edit `libs/` or `mgcxx/` — third-party, managed by the setup scripts.
- Do not commit before `./init-dev` has installed the hooks; a commit that skips pre-commit will fail CI on formatting alone.
