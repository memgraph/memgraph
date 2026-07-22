# A smoke test of Memgraph's release

The reasons for smoke release testing are:
* to test a given feature during the release cycle (the fastest is to directly build and run memgraph binary).
* to test packaged versions of Memgraph (e.g. Docker)
* to test on a given deployment infrastructure (e.g. k8s)
* to test under different infrastructure environments (e.g. k8s+GCP, k8s+Azure)
* to test backward compatibility
* to test Community -> Enterprise transition
* to test Enterprise -> Community transition.

NOTE: GQLAlchemy version is not fixed on purpose.
NOTE: GQLAlchmey uses an old version of the neo client -> the neo4j version is fixed.

## Test Types

* Testing drivers (mostly Memgraph ones, testing official Neo4j drives is done
under https://github.com/memgraph/memgraph/tree/master/tests/drivers + these
are run against the plain binary, not a full package).
* inspecting packaged files
* running queries to test that all features are correctly packaged
* migration procedures.

## Delivery Types

* Plain memgraph binary
* Linux packages (.deb, .rpm)
* Docker images
* Helm charts.

## Environments

* ARCH: x86, ARM
* OS: Linux, Mac, Windows
* K8s: Kind, Minicube
* Clouds: AWS, Azure, GCP.
