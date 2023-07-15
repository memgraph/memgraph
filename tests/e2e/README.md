# tests/e2e

Framework to run end-to-end tests against Memgraph.

## Notes

* If you change something under this directory and below (even a Python
  script), `make` has to be run again because all tests are copied to the build
  directory and executed from there.
* Use/extend `run.sh` if you run any e2e tests:
  * if all tests have to executed, use `run.sh`
  * if a suite of tests have to be execute, take a look under `run.sh` how to do so
  * if only a single test have to be execute, take a look at each individual binary/script, it's possible to manually pick the test
