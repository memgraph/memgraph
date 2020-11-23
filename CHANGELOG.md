# Change Log

## v1.2.0

### Breaking Changes

* SSL is disabled by default (`--bolt-cert-file` and `--bolt-key-file` are
  empty). This change might only affect the client connection configuration.

### Major Features and Improvements

* Added support for Bolt v4.0 and v4.1.
* Added `mgp_networkx.py` as an alternative implementation of NetworkX graph
  objects, which is useful to use Memgraph data from NetworkX algorithms
  optimally.
* Added `nxalg.py` query module as a proxy to NetworkX algorithms.
* Added plan optimization to use a label-property index where the property is
  not null. As a result, the query engine, instead of scanning all elements and
  applying the filter, performs a label-property index lookup when possible.

### Bug Fixes and Other Changes

* Fixed Cypher `ID` function `Null` handling. When the `ID` function receives
  `Null`, it will also return `Null`.
* Fixed bug that caused random crashes in SSL communication on platforms
  that use older versions of OpenSSL (< 1.1) by adding proper multi-threading
  handling.
* Fix `DISCARD` message handling. The query is now executed before discarding
  the results.

## v1.1.0

### Major Features and Improvements

* Properties in nodes and edges are now stored encoded and compressed. This
  change significantly reduces memory usage. Depending on the specific dataset,
  total memory usage can be reduced up to 50%.
* Added support for rescanning query modules. Previously, the query modules
  directory was scanned only upon startup. Now it is scanned each time the user
  requests to load a query module. The functions used to load the query modules
  were renamed to `mg.load()` and `mg.load_all()` (from `mg.reload()` and
  `mg.reload_all()`).
* Improved execution performance of queries that have an IN list filter by
  using label+property indices.
  Example: `MATCH (n:Label) WHERE n.property IN [] ...`
* Added support for `ANY` and `NONE` openCypher functions. Previously, only
  `ALL` and `SINGLE` functions were implemented.

### Bug Fixes and Other Changes

* Fixed invalid paths returned by variable expansion when the starting node and
  destination node used the same symbol.
  Example: `MATCH path = (n:Person {name: "John"})-[:KNOWS*]->(n) RETURN path`
* Improved semantics of `ALL` and `SINGLE` functions to be consistent with
  openCypher when handling lists with `Null`s.
* `SHOW CONSTRAINT INFO` now returns property names as a list for unique
  constraints.
* Escaped label/property/edgetype names in `DUMP DATABASE` to support names
  with spaces in them.
* Fixed handling of `DUMP DATABASE` queries in multi-command transactions
  (`BEGIN`, ..., `COMMIT`).
* Fixed handling of various query types in explicit transactions. For example,
  constraints were allowed to be created in multi-command transactions
  (`BEGIN`, ..., `COMMIT`) but that isn't a transactional operation and as such
  can't be allowed in multi-command transactions.
* Fixed integer overflow bugs in `COUNT`, `LIMIT` and `SKIP`.
* Fixed integer overflow bugs in weighted shortest path expansions.
* Fixed various other integer overflow bugs in query execution.
* Added Marvel Comic Universe tutorial.
* Added FootballTransfers tutorial.

## v1.0.0

### Major Features and Improvements

* [Enterprise Ed.] Exposed authentication username/rolename regex as a flag
  (`--auth-user-or-role-name-regex`).
* [Enterprise Ed.] Improved auth module error handling and added support for
  relative paths.
* Added support for Python query modules. This release of Memgraph supports
  query modules written using the already existing C API and the new Python
  API.
* Added support for unique constraints. The unique constraint is created with a
  label and one or more properties.
* Implemented support for importing CSV files (`mg_import_csv`). The importer
  is compatible with the Neo4j batch CSV importer.
* Snapshot and write-ahead log format changed (backward compatible with v0.50).
* Vertices looked up by their openCypher ID (`MATCH (n) WHERE ID(n) = ...`)
  will now find the node in O(logn) instead of O(n).
* Improved planning of BFS expansion, a faster, specific approach is now
  favored instead of a ScanAll+Filter operation.
* Added syntax for limiting memory of `CALL`.
* Exposed server name that should be used for Bolt handshake as flag
  (`--bolt-server-name-for-init`).
* Added several more functions to the query module C API.
* Implemented a storage locking mechanism that prevents the user from
  concurrently starting two Memgraph instances with the same data directory.

### Bug Fixes and Other Changes

* [Enterprise Ed.] Fixed a bug that crashed the database when granting
  privileges to a user.
* [Enterprise Ed.] Improved Louvain algorithm for community detection.
* Type of variable expansion is now printed in `EXPLAIN` (e.g. ExpandVariable,
  STShortestPath, BFSExpand, WeightedShortestPath).
* Correctly display `CALL` in `EXPLAIN` output.
* Correctly delimit arguments when printing the signature of a query module.
* Fixed a planning issue when `CALL` preceded filtering.
* Fixed spelling mistakes in the storage durability module.
* Fixed storage GC indices/constraints subtle race condition.
* Reduced memory allocations in storage API and indices.
* Memgraph version is now outputted to `stdout` when Memgraph is started.
* Improved RPM packaging.
* Reduced number of errors reported in production log when loading query
  modules.
* Removed `early access` wording from the Community Offering license.

## v0.50.0

### Breaking Changes

* [Enterprise Ed.] Remove support for Kafka streams.
* Snapshot and write-ahead log format changed (not backward compatible).
* Removed support for unique constraints.
* Label indices aren't created automatically, create them explicitly instead.
* Renamed several database flags. Please see the configuration file for a list of current flags.

### Major Features and Improvements

* [Enterprise Ed.] Add support for auth module.
* [Enterprise Ed.] LDAP support migrated to auth module.
* Implemented new graph storage engine.
* Add support for disabling properties on edges.
* Add support for existence constraints.
* Add support for custom openCypher procedures using a C API.
* Support loading query modules implementing read-only procedures.
* Add `CALL <procedure> YIELD <result>` syntax for invoking loaded procedures.
* Add `CREATE INDEX ON :Label` for creating label indices.
* Add `DROP INDEX ON :Label` for dropping label indices.
* Add `DUMP DATABASE` clause to openCypher.
* Add functions for treating character strings as byte strings.

### Bug Fixes and Other Changes

* Fix several memory management bugs.
* Reduce memory usage in query execution.
* Fix bug that crashes the database when `EXPLAIN` is used.

## v0.15.0

### Breaking Changes

* Snapshot and write-ahead log format changed (not backward compatible).
* `indexInfo()` function replaced with `SHOW INDEX INFO` syntax.
* Removed support for unique index. Use unique constraints instead.
* `CREATE UNIQUE INDEX ON :label (property)` replaced with `CREATE CONSTRAINT ON (n:label) ASSERT n.property IS UNIQUE`.
* Changed semantics for `COUNTER` openCypher function.

### Major Features and Improvements

* [Enterprise Ed.] Add new privilege, `STATS` for accessing storage info.
* [Enterprise Ed.] LDAP authentication and authorization support.
* [Enterprise Ed.] Add audit logging feature.
* Add multiple properties unique constraint which replace unique indices.
* Add `SHOW STORAGE INFO` feature.
* Add `PROFILE` clause to openCypher.
* Add `CREATE CONSTRAINT` clause to openCypher.
* Add `DROP CONSTRAINT` clause to openCypher.
* Add `SHOW CONSTRAINT INFO` feature.
* Add `uniformSample` function to openCypher.
* Add regex matching to openCypher.

### Bug Fixes and Other Changes

* Fix bug in explicit transaction handling.
* Fix bug in edge filtering by edge type and destination.
* Fix bug in query comment parsing.
* Fix bug in query symbol table.
* Fix OpenSSL memory leaks.
* Make authentication case insensitive.
* Remove `COALESCE` function.
* Add movie tutorial.
* Add backpacking tutorial.

## v0.14.0

### Breaking Changes

* Write-ahead log format changed (not backward compatible).

### Major Features and Improvements

* [Enterprise Ed.] Reduce memory usage in distributed usage.
* Add `DROP INDEX` feature.
* Improve SSL error messages.

### Bug Fixes and Other Changes

* [Enterprise Ed.] Fix issues with reading and writing in a distributed query.
* Correctly handle an edge case with unique constraint checks.
* Fix a minor issue with `mg_import_csv`.
* Fix an issue with `EXPLAIN`.

## v0.13.0

### Breaking Changes

* Write-ahead log format changed (not backward compatible).
* Snapshot format changed (not backward compatible).

### Major Features and Improvements

* [Enterprise Ed.] Authentication and authorization support.
* [Enterprise Ed.] Kafka integration.
* [Enterprise Ed.] Support dynamic worker addition in distributed.
* Reduce memory usage and improve overall performance.
* Add `CREATE UNIQUE INDEX` clause to openCypher.
* Add `EXPLAIN` clause to openCypher.
* Add `inDegree` and `outDegree` functions to openCypher.
* Improve BFS performance when both endpoints are known.
* Add new `node-label`, `relationship-type` and `quote` options to
  `mg_import_csv` tool.
* Reduce memory usage of `mg_import_csv`.

### Bug Fixes and Other Changes

* [Enterprise Ed.] Fix an edge case in distributed index creation.
* [Enterprise Ed.] Fix issues with Cartesian in distributed queries.
* Correctly handle large messages in Bolt protocol.
* Fix issues when handling explicitly started transactions in queries.
* Allow openCypher keywords to be used as variable names.
* Revise and make user visible error messages consistent.
* Improve aborting time consuming execution.

## v0.12.0

### Breaking Changes

* Snapshot format changed (not backward compatible).

### Major Features and Improvements

* Improved Id Cypher function.
* Added string functions to openCypher (`lTrim`, `left`, `rTrim`, `replace`,
 `reverse`, `right`, `split`, `substring`, `toLower`, `toUpper`, `trim`).
* Added `timestamp` function to openCypher.
* Added support for dynamic property access with `[]` operator.

## v0.11.0

### Major Features and Improvements

* [Enterprise Ed.] Improve Cartesian support in distributed queries.
* [Enterprise Ed.] Improve distributed execution of BFS.
* [Enterprise Ed.] Dynamic graph partitioner added.
* Static nodes/edges id generators exposed through the Id Cypher function.
* Properties on disk added.
* Telemetry added.
* SSL support added.
* `toString` function added.

### Bug Fixes and Other Changes

* Document issues with Docker on OS X.
* Add BFS and Dijkstra's algorithm examples to documentation.

## v0.10.0

### Breaking Changes

* Snapshot format changed (not backward compatible).

### Major Features and Improvements

* [Enterprise Ed.] Distributed storage and execution.
* `reduce` and `single` functions added to openCypher.
* `wShortest` edge expansion added to openCypher.
* Support packaging RPM on CentOS 7.

### Bug Fixes and Other Changes

* Report an error if updating a deleted element.
* Log an error if reading info on available memory fails.
* Fix a bug when `MATCH` would stop matching if a result was empty, but later
  results still contain data to be matched. The simplest case of this was the
  query: `UNWIND [1,2,3] AS x MATCH (n :Label {prop: x}) RETURN n`. If there
  was no node `(:Label {prop: 1})`, then the `MATCH` wouldn't even try to find
  for `x` being 2 or 3.
* Report an error if trying to compare a property value with something that
  cannot be stored in a property.
* Fix crashes in some obscure cases.
* Commit log automatically garbage collected.
* Add minor performance improvements.

## v0.9.0

### Breaking Changes

* Snapshot format changed (not backward compatible).
* Snapshot configuration flags changed, general durability flags added.

### Major Features and Improvements

* Write-ahead log added.
* `nodes` and `relationships` functions added.
* `UNION` and `UNION ALL` is implemented.
* Concurrent index creation is now enabled.

### Bug Fixes and Other Changes


## v0.8.0

### Major Features and Improvements

* CASE construct (without aggregations).
* Named path support added.
* Maps can now be stored as node/edge properties.
* Map indexing supported.
* `rand` function added.
* `assert` function added.
* `counter` and `counterSet` functions added.
* `indexInfo` function added.
* `collect` aggregation now supports Map collection.
* Changed the BFS syntax.

### Bug Fixes and Other Changes

* Use \u to specify 4 digit codepoint and \U for 8 digit
* Keywords appearing in header (named expressions) keep original case.
* Our Bolt protocol implementation is now completely compatible with the protocol version 1 specification. (https://boltprotocol.org/v1/)
* Added a log warning when running out of memory and the `memory_warning_threshold` flag
* Edges are no longer additionally filtered after expansion.

## v0.7.0

### Major Features and Improvements

* Variable length path `MATCH`.
* Explicitly started transactions (multi-query transactions).
* Map literal.
* Query parameters (except for parameters in place of property maps).
* `all` function in openCypher.
* `degree` function in openCypher.
* User specified transaction execution timeout.

### Bug Fixes and Other Changes

* Concurrent `BUILD INDEX` deadlock now returns an error to the client.
* A `MATCH` preceeded by `OPTIONAL MATCH` expansion inconsistencies.
* High concurrency Antlr parsing bug.
* Indexing improvements.
* Query stripping and caching speedups.

## v0.6.0

### Major Features and Improvements

* AST caching.
* Label + property index support.
* Different logging setup & format.

## v0.5.0

### Major Features and Improvements

* Use label indexes to speed up querying.
* Generate multiple query plans and use the cost estimator to select the best.
* Snapshots & Recovery.
* Abandon old yaml configuration and migrate to gflags.
* Query stripping & AST caching support.

### Bug Fixes and Other Changes

* Fixed race condition in MVCC. Hints exp+aborted race condition prevented.
* Fixed conceptual bug in MVCC GC. Evaluate old records w.r.t. the oldest.
  transaction's id AND snapshot.
* User friendly error messages thrown from the query engine.

## Build 837

### Bug Fixes and Other Changes

* List indexing supported with preceeding IN (for example in query `RETURN 1 IN [[1,2]][0]`).

## Build 825

### Major Features and Improvements

* RETURN *, count(*), OPTIONAL MATCH, UNWIND, DISTINCT (except DISTINCT in aggregate functions), list indexing and slicing, escaped labels, IN LIST operator, range function.

### Bug Fixes and Other Changes

* TCP_NODELAY -> import should be faster.
* Clear hint bits.

## Build 783

### Major Features and Improvements

* SKIP, LIMIT, ORDER BY.
* Math functions.
* Initial support for MERGE clause.

### Bug Fixes and Other Changes

* Unhandled Lock Timeout Exception.

## Build 755

### Major Features and Improvements

* MATCH, CREATE, WHERE, SET, REMOVE, DELETE.
