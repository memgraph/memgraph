# mgcxx (experimental)

## Merging mgcxx and Mage into memgraph/memgraph repo (November 7th, 2025)

Hi Memgraph Community!

Quick heads up / announcement. We have decided to merge the [memgraph/mage](http://github.com/memgraph/mage) and [memgraph/mgcxx](http://github.com/memgraph/mgcxx) repositories into [memgraph/memgraph](http://github.com/memgraph/memgraph) at some point in the near future (within the next few months).

Let me outline the reasoning. Despite Memgraph's usage growing in the last few years, we have encouraged the community to make external open source contributions, but the impact of these contributions has remained relatively insignificant. On the other hand, we see huge potential in the merger because it will allow us to have more efficient and faster release cycles. A better release cycle means more and improved capabilities from Memgraph.

Regarding licensing, at the time of the merge, merged repositories will be archived under the existing license. In contrast, the merged code will be released under the existing Memgraph Community BSL license. Effectively integrating the merged code into the Memgraph Community. The merged code, as well as existing Memgraph Community code, will be available under the BSL license, offering the most important open source benefits: right to inspect, right to repair, and right to improve. The external contributions are always welcome. In fact, we'll also make the process of contributing easier.

From a usage perspective, Memgraph packages and Docker images will remain unchanged. A separate Docker image will still be available, including all Mage modules. mgcxx is already statically linked (it’s included in all Memgraph packages). Over the long run, we plan to introduce a package manager for all Memgraph modules.

I hope all the above makes sense. We'll keep you posted about the progress. To give feedback, please visit [the discussion](https://github.com/memgraph/memgraph/discussions/3413).

Best, Marko ([gitbuda](http://github.com/gitbuda)), CTO @ Memgraph

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
