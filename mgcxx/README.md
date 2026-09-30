# mgcxx

**NOTE**: As of January 2026, MGCXX has become part of the Memgraph repository;
the previously separate MGCXX repository can be found
[here](https://github.com/memgraph/mgcxx.git) and is now archived.

--------

A collection of C++ wrappers around non-C++ libraries.
The list includes:
  * full-text search enabled by [tantivy](https://github.com/quickwit-oss/tantivy)

Requirements:
  * cmake 3.15+
  * rustup toolchain 1.75.0+

## How to build and test?

```
mkdir build && cd build
cmake ..
make && ctest
```

## text_search

### TODOs

- [ ] Polish & test all error messages
- [ ] Write unit / integration test to compare STRING vs JSON fiels search query syntax.
- [ ] Figure out what's the right search syntax for a property graph
- [ ] Add some notion of pagination
- [ ] Add some notion of backwards compatiblity -> some help to the user
- [ ] How to:
    - [ ] search all properties
    - [ ] fuzzy search
          ```
          // let term = Term::from_field_text(data_field, &input.search_query);
          // let query = FuzzyTermQuery::new(term, 2, true);
          ```
- [ ] Add Github Actions
- [ ] Add benchmarks:
    - [ ] Test what's the tradeoff between searching STRING vs JSON TEXT, how does the query look like?
    - [ ] Search direct field vs JSON, FAST vs SLOW, String vs CxxString
    - [ ] MATCH (n) RETURN count(n), n.deleted;
    - [ ] search of a specific property value
    - [ ] benchmark (add|retrieve simple|complex, filtering, aggregations).
    - [ ] search of all properties
    - [ ] Benchmark (search by GID to get document_id + fetch document by document_id) vs (fetch document by document_id) on 100M nodes + 100M edges
        - [ ] Note [DocAddress](https://docs.rs/tantivy/latest/tantivy/struct.DocAddress.html) is composed of 2 u32 but the `SegmentOrdinal` is tied to the `Searcher` -> is it possible/wise to cache the address (`SegmentId` is UUID)
            - [ ] A [searcher](https://docs.rs/tantivy/latest/tantivy/struct.IndexReader.html#method.searcher) per transaction -> cache `DocAddress` inside Memgraph's `ElementAccessors`?
- [ ] Implement the stress test by adding & searching to the same index concurrently + large dataset generator.
- [ ] Consider implementing panic! handler preventing outside process to crash (optionally).

### NOTEs

* if a field doesn't get specified in the schema, it's ignored
* `TEXT` means the field will be tokenized and indexed (required to be able to
  search)
* Tantivy add_json_object accepts serde_json::map::Map<String, serde_json::value::Value>
* C++ text-search API is snake case because it's implemented in Rust
* Writing each document and then committing (writing to disk) will be
  expensive. In a standard OLTP workload that's a common case -> introduce some
  form of batching.

## Resources

* https://fulmicoton.com/posts/behold-tantivy-part2
* https://stackoverflow.com/questions/37924383/combining-several-static-libraries-into-one-using-cmake
    --> decided to have 2 separate libraries user code has to link
