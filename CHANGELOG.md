# Change Log

## Next version

### Major Features and Improvements

* CASE construct (without aggregations).
* `rand` function added.
* Maps can now be stored as vertex/edge properties.
* `collect` aggregation now supports Map collection.

### Bug Fixes and Other Changes

* Keywords appearing in header (named expressions) keep original case.

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
