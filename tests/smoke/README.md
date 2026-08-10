# A smoke test of Memgraph's release

The reasons for smoke release testing are:
* to test a given feature during the release cycle (the fastest is to directly build and run memgraph binary).
* to test packaged versions of Memgraph (e.g. Docker)
* to test Community -> Enterprise transition
* to test Enterprise -> Community transition.

There are two paths:
* `./test_single.bash` — tests a single running Docker image (the image under
  test, `MEMGRAPH_DOCKERHUB_IMAGE`). This is what CI runs; it needs nothing but
  Docker.
* `./test_k8s.bash` — tests the same image deployed on Kubernetes (kind + the
  memgraph helm charts), both a single instance and an HA cluster. MANUAL only,
  not run in CI. Install the tooling once with `./k8s/init.bash`.

Upgrade/backwards-compatibility testing is NOT done here, it lives under
`tests/issu`.

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

## Delivery Types (manual)

* Helm charts (see `./test_k8s.bash`).

## Environments

* ARCH: x86, ARM
* OS: Linux, Mac, Windows
* K8s: Kind (manual only).

## How to run

```
./init.bash            # or init_mac.bash; downloads mgconsole + builds the C++ query module
./test_single.bash memgraph|mage
```

On Kubernetes (manual):

```
./k8s/init.bash        # installs kind/kubectl/helm and creates the kind cluster
./test_k8s.bash
```
