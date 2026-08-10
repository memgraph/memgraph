# A smoke test of Memgraph's release

The reasons for smoke release testing are:
* to test a given feature during the release cycle (the fastest is to directly build and run memgraph binary).
* to test packaged versions of Memgraph (e.g. Docker)
* to test Community -> Enterprise transition
* to test Enterprise -> Community transition.

The tests run against a single running Docker image (the image under test,
`MEMGRAPH_DOCKERHUB_IMAGE`). Upgrade/backwards-compatibility testing is
NOT done here, it lives under `tests/issu`.

NOTE: GQLAlchemy version is not fixed on purpose.
NOTE: GQLAlchmey uses an old version of the neo client -> the neo4j version is fixed.

## Test Types

* Testing drivers (mostly Memgraph ones, testing official Neo4j drives is done
under https://github.com/memgraph/memgraph/tree/master/tests/drivers + these
are run against the plain binary, not a full package).
* inspecting packaged files
* running queries to test that all features are correctly packaged.

## Delivery Types

* Plain memgraph binary
* Linux packages (.deb, .rpm)
* Docker images.

## Environments

* ARCH: x86, ARM
* OS: Linux, Mac, Windows.

## How to run

```
./init.bash            # or init_mac.bash; downloads mgconsole + builds the C++ query module
./test_single.bash memgraph|mage
```
