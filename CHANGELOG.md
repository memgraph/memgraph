# Change Log

## Next Build

### Major Features and Improvements

TODO

### Bug Fixes and Other Changes

TODO

## Build 837

### Bug Fixes and Other Changes

* List indexing supported with preceeding IN (for example in query `RETURN 1 IN [[1,2]][0]`)

## Build 825

### Major Features and Improvements

* RETURN *, count(*), OPTIONAL MATCH, UNWIND, DISTINCT (except DISTINCT in aggregate functions), list indexing and slicing, escaped labels, IN LIST operator, range function

### Bug Fixes and Other Changes

* TCP_NODELAY -> import should be faster
* Clear hint bits

## Build 783

### Major Features and Improvements

* SKIP, LIMIT, ORDER BY
* Math functions
* Initial support for MERGE clause

### Bug Fixes and Other Changes

* Unhandled Lock Timeout Exception

## Build 755

### Major Features and Improvements

* MATCH, CREATE, WHERE, SET, REMOVE, DELETE
