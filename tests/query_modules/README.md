# Query modules e2e tests

This is the same testing infrastructure as on MAGE.
Three types of tests exist, static, online and export.
For all tests create a directory that ends with `_test`.

## Static tests

Static tests verify query results by comparing their output to predefined
expected results.  Create a separate folder for each test in the directory.

The directory contains two files: `input.cyp` which contains Cypher queries to
create the initial graph state and `test.yml` which defines the test query and
its expected results. Look at `example_test` folder for examples.

## Online tests

Online tests validate dynamic changes to the graph over a series of queries.
Create a test directory whose name starts with `test_online`.

In `input.cyp` define setup, checkpoint and cleanup queries. In `test.yml`
define expected results. Look at `example_online_test` for examples.

## Export tests
TODO: not important now

## System Packages

Memgraph loads Python query modules and assumes deps are installed globally
(compile-time config). That's why Python deps also have to be installed
globally to make tests work.
