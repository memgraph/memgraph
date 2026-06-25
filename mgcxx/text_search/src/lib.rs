// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

use log::debug;
use serde_json::{to_string, Value};
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::AggregationCollector;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{QueryParser, RegexQuery};
use tantivy::schema::*;
use tantivy::{DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, TERMINATED};
use tantivy_fst::Regex;

// NOTE: Result<T> == Result<T,std::io::Error>.
#[cxx::bridge(namespace = "mgcxx::text_search")]
mod ffi {
    // TODO(gitbuda): Try to put direct pointers to the tantivy datastructures under Context
    struct Context {
        tantivyContext: Box<TantivyContext>,
    }

    /// mappings format (JSON string expected):
    ///   {
    ///     "properties": {
    ///       "{{field_name}}": {
    ///         "type": "{{bool|json|text|u64}}",
    ///         "fast": {{true|false}},
    ///         "indexed": {{true|false}},
    ///         "stored": {{true|false}},
    ///         "text": {{true|false}},
    ///       }
    ///     }
    ///   }
    /// NOTE: "properties" is just taken to be similar with other text search engines, exact
    /// senamtics might be different.
    struct IndexConfig {
        mappings: String,
        // TODO(gitbuda): Add tokenizer as an option (each field can have one).
    }

    struct DocumentInput {
        /// JSON encoded string with data.
        /// Mappings inside IndexConfig defines how data will be handeled.
        data: String,
    }
    // NOTE: The input struct is/should_be aligned with the schema.
    struct DocumentOutput {
        data: String, // NOTE: Here should probably be Option but it's not supported in cxx.
        score: f32,   // Relevance score from search
    }

    struct SearchInput {
        search_fields: Vec<String>,
        search_query: String,
        return_fields: Vec<String>,
        aggregation_query: String,
        limit: usize,
        fuzzy_distance: u8,
        fuzzy_prefix: bool,
        fuzzy_transpositions: bool,
        fuzzy_field: String,
        // NOTE: Any primitive value here is a bit of a problem because of default value on the C++
        // side.
    }

    struct GidScore {
        gid: u64,
        score: f32,
    }
    struct GidScoreOutput {
        docs: Vec<GidScore>,
    }

    struct EdgeGidScore {
        edge_gid: u64,
        from_gid: u64,
        to_gid: u64,
        score: f32,
    }
    struct EdgeGidScoreOutput {
        docs: Vec<EdgeGidScore>,
    }

    // NOTE: Since return type is Result<T>, always return Result<Something>.
    extern "Rust" {
        type TantivyContext;
        type SearcherContext;
        fn init(_log_level: &String) -> Result<()>;
        /// path is just passed into std::path::Path::new -> pass any absolute or relative path to
        /// yours process working directory
        /// config contains mappings definition, take a look under [IndexConfig]
        fn create_index(path: &String, config: &IndexConfig) -> Result<Context>;
        fn add_document(
            context: &mut Context,
            input: &DocumentInput,
            skip_commit: bool,
        ) -> Result<()>;
        fn delete_document(
            context: &mut Context,
            input: &SearchInput,
            skip_commit: bool,
        ) -> Result<()>;
        fn commit(context: &mut Context) -> Result<()>;
        fn rollback(context: &mut Context) -> Result<()>;
        // NOTE: search() and regex_search() are not currently called from C++ — the optimized
        // *_gids_pinned variants are used instead. Keeping these because we will need them once we
        // add single-store mode to the text index (returning full documents from Tantivy).
        // NOTE: the read-only entry points take `&Context` (shared) on purpose: they never mutate
        // the index and must be safe to call concurrently on the same shared Context from multiple
        // threads (the C++ side does exactly this, without a lock). Mutating ops (add/delete/commit/
        // rollback) keep `&mut Context` and are serialized by the C++ write_mutex.
        fn aggregate(context: &Context, input: &SearchInput) -> Result<DocumentOutput>;
        fn acquire_searcher(context: &Context) -> Result<Box<SearcherContext>>;
        fn search_gids_pinned(
            context: &Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<GidScoreOutput>;
        fn regex_search_gids_pinned(
            context: &Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<GidScoreOutput>;
        fn search_edge_gids_pinned(
            context: &Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<EdgeGidScoreOutput>;
        fn regex_search_edge_gids_pinned(
            context: &Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<EdgeGidScoreOutput>;
        fn get_num_docs(context: &Context) -> Result<u64>;
        fn drop_index(context: Context) -> Result<()>;
    }
}

impl ffi::SearchInput {
    fn effective_limit(&self) -> usize {
        if self.limit == 0 { 1000 } else { self.limit }
    }
}

fn apply_fuzzy_config(
    query_parser: &mut QueryParser,
    schema: &Schema,
    input: &ffi::SearchInput,
) -> Result<(), std::io::Error> {
    if input.fuzzy_distance == 0 {
        return Ok(());
    }
    let field = schema.get_field(&input.fuzzy_field).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("fuzzy field '{}' not found in schema -> {}", input.fuzzy_field, e),
        )
    })?;
    query_parser.set_field_fuzzy(
        field,
        input.fuzzy_prefix,
        input.fuzzy_distance,
        input.fuzzy_transpositions,
    );
    Ok(())
}

fn owned_value_to_json(val: OwnedValue) -> serde_json::Value {
    match val {
        OwnedValue::Null => serde_json::Value::Null,
        OwnedValue::Str(s) => serde_json::Value::String(s),
        OwnedValue::U64(n) => serde_json::json!(n),
        OwnedValue::I64(n) => serde_json::json!(n),
        OwnedValue::F64(n) => serde_json::json!(n),
        OwnedValue::Bool(b) => serde_json::Value::Bool(b),
        OwnedValue::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(owned_value_to_json).collect())
        }
        OwnedValue::Object(entries) => {
            let map: serde_json::Map<String, serde_json::Value> = entries
                .into_iter()
                .map(|(k, v)| (k, owned_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        other => match serde_json::to_value(&other) {
            Ok(v) => v,
            Err(e) => {
                log::error!(
                    "Failed to serialize OwnedValue to JSON in owned_value_to_json: value={:?}, error={}",
                    other,
                    e
                );
                serde_json::Value::Null
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_owned_value_to_json_nested() {
        let doc = OwnedValue::Object(vec![
            ("name".to_string(), OwnedValue::Str("test".to_string())),
            ("count".to_string(), OwnedValue::I64(-3)),
            ("tags".to_string(), OwnedValue::Array(vec![
                OwnedValue::Bool(true),
                OwnedValue::U64(42),
            ])),
            ("nested".to_string(), OwnedValue::Object(vec![
                ("x".to_string(), OwnedValue::F64(1.5)),
            ])),
            ("empty".to_string(), OwnedValue::Null),
        ]);
        assert_eq!(
            owned_value_to_json(doc),
            json!({"name": "test", "count": -3, "tags": [true, 42u64], "nested": {"x": 1.5}, "empty": null})
        );
    }

    #[test]
    fn regex_search_cache_returns_consistent_results() {
        let dir = std::env::temp_dir().join(format!("mgcxx_regex_cache_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.to_str().unwrap().to_string();
        let mappings = r#"{"properties":{
            "data":{"type":"json","fast":true,"stored":true,"text":true},
            "all":{"type":"text","fast":true,"stored":true,"text":true},
            "gid":{"type":"u64","fast":true,"stored":true,"indexed":true}}}"#
            .to_string();
        let mut ctx = create_index(&path, &ffi::IndexConfig { mappings }).unwrap();
        for (gid, name) in [(1u64, "alpha"), (2, "alpine"), (3, "beta")] {
            let data = format!(r#"{{"data":{{"name":"{name}"}},"all":"{name}","gid":{gid}}}"#);
            add_document(&mut ctx, &ffi::DocumentInput { data }, true).unwrap();
        }
        commit(&mut ctx).unwrap();
        let searcher = acquire_searcher(&ctx).unwrap();
        let input = |q: &str| ffi::SearchInput {
            search_fields: vec!["all".to_string()],
            search_query: q.to_string(),
            return_fields: vec![],
            aggregation_query: String::new(),
            limit: 10,
            fuzzy_distance: 0,
            fuzzy_prefix: false,
            fuzzy_transpositions: false,
            fuzzy_field: String::new(),
        };
        let gids = |out: ffi::GidScoreOutput| {
            let mut g: Vec<u64> = out.docs.iter().map(|d| d.gid).collect();
            g.sort_unstable();
            g
        };
        let miss = gids(regex_search_gids_pinned(&ctx, &searcher, &input("alp.*")).unwrap());
        let hit = gids(regex_search_gids_pinned(&ctx, &searcher, &input("alp.*")).unwrap());
        assert_eq!(miss, vec![1, 2]);
        assert_eq!(miss, hit);
        assert_eq!(gids(regex_search_gids_pinned(&ctx, &searcher, &input("bet.*")).unwrap()), vec![3]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Locks the two tantivy behaviors regex_search_gids_pinned hand-couples to:
    // (a) RegexQuery is constant-scored (the score=1.0 we emit), and (b) our manual
    // FST/postings/alive-bitset walk yields the same doc set as tantivy's own RegexQuery.
    // A tantivy upgrade that changes either fails here instead of silently diverging.
    #[test]
    fn regex_manual_iteration_matches_tantivy_regexquery() {
        let dir = std::env::temp_dir().join(format!("mgcxx_regex_lock_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.to_str().unwrap().to_string();
        let mappings = r#"{"properties":{
            "data":{"type":"json","fast":true,"stored":true,"text":true},
            "all":{"type":"text","fast":true,"stored":true,"text":true},
            "gid":{"type":"u64","fast":true,"stored":true,"indexed":true}}}"#
            .to_string();
        let mut ctx = create_index(&path, &ffi::IndexConfig { mappings }).unwrap();
        for (gid, name) in [
            (1u64, "alpha"), (2, "alpine"), (3, "algol"), (4, "alabama"),
            (5, "beta"), (6, "gamma"), (7, "alto"), (8, "almond"),
        ] {
            let data = format!(r#"{{"data":{{"name":"{name}"}},"all":"{name}","gid":{gid}}}"#);
            add_document(&mut ctx, &ffi::DocumentInput { data }, true).unwrap();
        }
        commit(&mut ctx).unwrap();

        let input = |q: &str, limit: usize| ffi::SearchInput {
            search_fields: vec!["all".to_string()],
            search_query: q.to_string(),
            return_fields: vec![],
            aggregation_query: String::new(),
            limit,
            fuzzy_distance: 0,
            fuzzy_prefix: false,
            fuzzy_transpositions: false,
            fuzzy_field: String::new(),
        };
        // soft-delete a match to exercise the alive-bitset path
        delete_document(&mut ctx, &input("almond", 10), false).unwrap();

        let searcher = acquire_searcher(&ctx).unwrap();
        let all_field = ctx.tantivyContext.schema.get_field("all").unwrap();

        let manual = |pattern: &str, limit: usize| {
            let mut g: Vec<u64> = regex_search_gids_pinned(&ctx, &searcher, &input(pattern, limit))
                .unwrap()
                .docs
                .iter()
                .map(|d| d.gid)
                .collect();
            g.sort_unstable();
            g
        };
        let canonical = |pattern: &str| {
            let query = RegexQuery::from_pattern(pattern, all_field).unwrap();
            let hits = searcher.searcher.search(&query, &TopDocs::with_limit(1000)).unwrap();
            for (score, _) in &hits {
                assert_eq!(*score, 1.0, "RegexQuery is no longer constant-scored");
            }
            let mut g: Vec<u64> = hits
                .iter()
                .map(|(_, addr)| {
                    searcher
                        .searcher
                        .segment_reader(addr.segment_ord)
                        .fast_fields()
                        .u64("gid")
                        .unwrap()
                        .first(addr.doc_id)
                        .unwrap()
                })
                .collect();
            g.sort_unstable();
            g
        };

        for pattern in ["al.*", "alp.*", "a.*a", ".*o.*", "zzz.*"] {
            assert_eq!(
                manual(pattern, 1000),
                canonical(pattern),
                "manual regex iteration diverged from RegexQuery for /{pattern}/"
            );
        }
        // the soft-deleted match (almond = gid 8) must not surface
        assert!(!manual("al.*", 1000).contains(&8));

        let capped = manual("al.*", 2);
        let full = canonical("al.*");
        assert_eq!(capped.len(), 2);
        assert!(capped.iter().all(|g| full.contains(g)));

        let _ = std::fs::remove_dir_all(&dir);
    }
}

// Field order is load-bearing: Rust drops fields in declaration order;
// index_reader must drop before index.
pub struct TantivyContext {
    pub index_path: std::path::PathBuf,
    pub schema: Schema,
    pub index_writer: Option<IndexWriter>,
    pub index_reader: IndexReader,
    pub index: Index,
    // compiled-regex automaton cache, keyed by pattern string; see compiled_regex()
    pub regex_cache: Mutex<lru::LruCache<String, Arc<Regex>>>,
}

impl TantivyContext {
    fn writer_mut(&mut self) -> &mut IndexWriter {
        self.index_writer.as_mut().expect("BUG: index_writer consumed before Drop")
    }
}

impl Drop for TantivyContext {
    fn drop(&mut self) {
        if let Some(writer) = self.index_writer.take() {
            if let Err(e) = writer.wait_merging_threads() {
                log::warn!(
                    "wait_merging_threads failed during TantivyContext drop for {:?}: {:?}",
                    self.index_path,
                    e
                );
            }
        }
    }
}

fn init(_log_level: &String) -> Result<(), std::io::Error> {
    // NOTE: Logger format is not aligned with the C++ host application.
    let log_init_res = env_logger::try_init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "warn"),
    );
    // NOTE: If more than one module calls env_logger::try_init, the later call will fail because
    // the logger is already initialized. Currently we treat this as a hard error; if this becomes
    // a problem, consider ignoring the AlreadyInitialized variant.
    if let Err(e) = log_init_res {
        return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to initialize tantivy (text search engine) logger -> {} -> you should probably stop your entire process and make sure it can be initialized properly.", e),
        ));
    }
    Ok(())
}

fn create_index_schema(
    mappings: &serde_json::Map<String, Value>,
) -> Result<Schema, std::io::Error> {
    let mut schema_builder = Schema::builder();
    if let Some(properties) = mappings.get("properties") {
        if let Some(properties_map) = properties.as_object() {
            for (field_name, value) in properties_map {
                let field_type = match value.get("type") {
                    Some(r) => match r.as_str() {
                        Some(s) => s,
                        None => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "field type should be a string",
                            ));
                        }
                    },
                    None => {
                        return Err(Error::new(ErrorKind::Other, "field should have a type"));
                    }
                };
                let is_stored = match value.get("stored") {
                    Some(r) => match r.as_bool() {
                        Some(s) => s,
                        None => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "field -> stored should be bool",
                            ));
                        }
                    },
                    None => false,
                };
                let is_fast = match value.get("fast") {
                    Some(r) => match r.as_bool() {
                        Some(s) => s,
                        None => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "field -> fast should be bool",
                            ));
                        }
                    },
                    None => false,
                };
                let is_text = match value.get("text") {
                    Some(r) => match r.as_bool() {
                        Some(s) => s,
                        None => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "field -> text should be bool",
                            ));
                        }
                    },
                    None => false,
                };
                let is_indexed = match value.get("indexed") {
                    Some(r) => match r.as_bool() {
                        Some(s) => s,
                        None => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                "field -> indexed should be bool",
                            ));
                        }
                    },
                    None => false,
                };
                match field_type {
                    "u64" => {
                        let mut options = NumericOptions::default();
                        if is_stored {
                            options = options.set_stored();
                        }
                        if is_fast {
                            options = options.set_fast();
                        }
                        if is_indexed {
                            options = options.set_indexed();
                        }
                        schema_builder.add_u64_field(field_name, options);
                    }
                    "text" => {
                        let mut options = TextOptions::default();
                        if is_stored {
                            options = options.set_stored();
                        }
                        if is_fast {
                            options = options.set_fast(None);
                        }
                        if is_text {
                            options = options | TEXT
                        }
                        schema_builder.add_text_field(field_name, options);
                    }
                    "json" => {
                        let mut options = JsonObjectOptions::default();
                        if is_stored {
                            options = options.set_stored();
                        }
                        if is_fast {
                            options = options.set_fast(None);
                        }
                        if is_text {
                            options = options | TEXT
                        }
                        schema_builder.add_json_field(field_name, options);
                    }
                    "bool" => {
                        let mut options = NumericOptions::default();
                        if is_stored {
                            options = options.set_stored();
                        }
                        if is_fast {
                            options = options.set_fast();
                        }
                        if is_indexed {
                            options = options.set_indexed();
                        }
                        schema_builder.add_bool_field(field_name, options);
                    }
                    _ => {
                        return Err(Error::new(ErrorKind::Other, "unknown field type"));
                    }
                }
            }
        } else {
            return Err(Error::new(
                ErrorKind::Other,
                "mappings has to contain properties",
            ));
        }
    } else {
        return Err(Error::new(
            ErrorKind::Other,
            "mappings has to contain properties",
        ));
    }
    let schema = schema_builder.build();
    Ok(schema)
}

fn create_index_dir_structure(
    path: &String,
    schema: &Schema,
) -> Result<(Index, std::path::PathBuf), std::io::Error> {
    let index_path = std::path::Path::new(path);
    if !index_path.exists() {
        match std::fs::create_dir_all(index_path) {
            Ok(_) => {
                debug!("{:?} folder created", index_path);
            }
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to create {:?} text search index folder -> {}",
                        index_path, e
                    ),
                ));
            }
        }
    }
    let mmap_directory = match MmapDirectory::open(&index_path) {
        Ok(d) => d,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Failed to mmap text search index folder at {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    };
    // NOTE: If schema doesn't match, open_or_create is going to return an error.
    let index = match Index::open_or_create(mmap_directory, schema.clone()) {
        Ok(index) => index,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to initialize text search index under {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    };
    Ok((index, index_path.to_path_buf()))
}

fn create_index(path: &String, config: &ffi::IndexConfig) -> Result<ffi::Context, std::io::Error> {
    let mappings = match serde_json::from_str::<serde_json::Map<String, Value>>(&config.mappings) {
        Ok(r) => r,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to parse mappings for index at {} -> {}", path, e),
            ));
        }
    };
    let schema = create_index_schema(&mappings)?;
    let (index, path) = create_index_dir_structure(path, &schema)?;
    let index_writer: IndexWriter = match index.writer(50_000_000) {
        Ok(writer) => writer,
        Err(e) => {
            return Err(Error::new(ErrorKind::Other, format!("Unable to initialize {:?} text search index writer -> {} This happened during the index creation. Make sure underlying machine is properly configured and try to execute create index again.", path, e)));
        }
    };

    // Create index reader with manual reload policy
    let index_reader = match index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
    {
        Ok(reader) => reader,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to create index reader for {:?} -> {}", path, e),
            ));
        }
    };

    Ok(ffi::Context {
        tantivyContext: Box::new(TantivyContext {
            index_path: path,
            schema,
            index_writer: Some(index_writer),
            index_reader,
            index,
            regex_cache: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(REGEX_CACHE_CAPACITY).expect("REGEX_CACHE_CAPACITY must be non-zero"),
            )),
        }),
    })
}

fn add_document(
    context: &mut ffi::Context,
    input: &ffi::DocumentInput,
    skip_commit: bool,
) -> Result<(), std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;
    let document = match TantivyDocument::parse_json(&schema, &input.data) {
        Ok(json) => json,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to add document into text search index {:?} because schema doesn't match -> {} Please check mappings.",
                    index_path, e
                ),
            ));
        }
    };
    match context.tantivyContext.writer_mut().add_document(document) {
        Ok(_) => {
            if skip_commit {
                return Ok(());
            } else {
                commit(context)
            }
        }
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to add document -> {}", e),
            ));
        }
    }
}

fn delete_document(
    context: &mut ffi::Context,
    input: &ffi::SearchInput,
    skip_commit: bool,
) -> Result<(), std::io::Error> {
    let query = {
        let index_path = &context.tantivyContext.index_path;
        let index = &context.tantivyContext.index;
        let schema = &context.tantivyContext.schema;
        let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
        let query_parser = QueryParser::for_index(index, search_fields);
        match query_parser.parse_query(&input.search_query) {
            Ok(q) => q,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Unable to create search query for {:?} text search index -> {}",
                        index_path, e
                    ),
                ));
            }
        }
    };
    match context.tantivyContext.writer_mut().delete_query(query) {
        Ok(_) => {
            if skip_commit {
                return Ok(());
            } else {
                commit(context)
            }
        }
        Err(e) => {
            let index_path = &context.tantivyContext.index_path;
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to delete document from text search index at {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    }
}

fn commit(context: &mut ffi::Context) -> Result<(), std::io::Error> {
    match context.tantivyContext.writer_mut().commit() {
        Ok(_) => {
            // Explicitly reload the index reader to see the new changes
            let index_path = &context.tantivyContext.index_path;
            if let Err(e) = context.tantivyContext.index_reader.reload() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Unable to reload reader after commit for text search index at {:?} -> {}",
                        index_path, e
                    ),
                ));
            }
            return Ok(());
        }
        Err(e) => {
            let index_path = &context.tantivyContext.index_path;
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to commit text search index at {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    }
}

fn rollback(context: &mut ffi::Context) -> Result<(), std::io::Error> {
    match context.tantivyContext.writer_mut().rollback() {
        Ok(_) => {
            return Ok(());
        }
        Err(e) => {
            let index_path = &context.tantivyContext.index_path;
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to rollback text search index at {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    }
}

fn search_get_fields(
    fields: &Vec<String>,
    schema: &Schema,
    index_path: &std::path::PathBuf,
) -> Result<Vec<Field>, std::io::Error> {
    let mut result: Vec<Field> = Vec::new();
    result.reserve(fields.len());
    for name in fields {
        match schema.get_field(name) {
            Ok(f) => result.push(f),
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("{} inside {:?} text search index", e, index_path),
                ));
            }
        }
    }
    Ok(result)
}

pub struct SearcherContext {
    searcher: tantivy::Searcher,
}

fn acquire_searcher(
    context: &ffi::Context,
) -> Result<Box<SearcherContext>, std::io::Error> {
    let searcher = context.tantivyContext.index_reader.searcher();
    Ok(Box::new(SearcherContext { searcher }))
}

fn search_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::GidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let mut query_parser = QueryParser::for_index(index, search_fields);
    apply_fuzzy_config(&mut query_parser, schema, input)?;
    let query = query_parser.parse_query(&input.search_query).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Unable to create search query for {:?} -> {}", index_path, e),
        )
    })?;

    let top_docs = searcher_ctx
        .searcher
        .search(&query, &TopDocs::with_limit(input.effective_limit()))
        .map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to perform text search under {:?} -> {}", index_path, e),
            )
        })?;

    // Read `gid` from the columnar fast field instead of fetching + decompressing the full stored
    // document per hit (the stored `data` blob holds all indexed property values). `gid` is
    // configured as a fast field, so this is a direct columnar lookup.
    let gid_columns = searcher_ctx
        .searcher
        .segment_readers()
        .iter()
        .map(|sr| sr.fast_fields().u64("gid"))
        .collect::<Result<Vec<_>, tantivy::TantivyError>>()
        .map_err(|e| {
            Error::new(ErrorKind::Other, format!("gid fast field not available in {:?} -> {}", index_path, e))
        })?;

    let mut docs: Vec<ffi::GidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let gid = gid_columns[doc_address.segment_ord as usize]
            .first(doc_address.doc_id)
            .ok_or_else(|| {
                Error::new(ErrorKind::Other, format!("gid missing for matched doc in {:?}", index_path))
            })?;
        docs.push(ffi::GidScore { gid, score });
    }
    Ok(ffi::GidScoreOutput { docs })
}

const REGEX_CACHE_CAPACITY: usize = 256;

// Compiling a regex into an FST automaton is a pure function of the pattern but costs ~18% of a
// regex query at high throughput. Cache compiled automata by pattern string (bounded LRU);
// autocomplete reuses patterns heavily. On a miss we compile off-lock and only hold the lock for
// the small get/put. The automaton is independent of index/searcher, so caching by pattern is exact.
fn compiled_regex(context: &ffi::Context, pattern: &str) -> Result<Arc<Regex>, std::io::Error> {
    if let Some(regex) = context.tantivyContext.regex_cache.lock().unwrap().get(pattern) {
        return Ok(regex.clone());
    }
    let regex = Arc::new(Regex::new(pattern).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!(
                "Unable to create regex search query for {:?} -> {}",
                context.tantivyContext.index_path, e
            ),
        )
    })?);
    context
        .tantivyContext
        .regex_cache
        .lock()
        .unwrap()
        .put(pattern.to_string(), regex.clone());
    Ok(regex)
}

fn regex_search_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::GidScoreOutput, std::io::Error> {
    // Regex queries in tantivy produce a constant score (ConstScorer) for every matching doc, so
    // there is no meaningful ranking to preserve. Instead of letting RegexQuery+TopDocs eagerly
    // materialize the full matching doc set into a max_doc-sized bitset (see AutomatonWeight::scorer),
    // we stream the FST-pruned matching terms and their postings lazily and stop as soon as we have
    // collected `limit` distinct docs. This turns the cost from O(all matching docs) into O(limit)
    // for prefix-style patterns that fan out across many terms.
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;

    let search_field = search_get_fields(&input.search_fields, schema, index_path)?[0];
    let regex = compiled_regex(context, &input.search_query)?;

    let limit = input.effective_limit();
    let mut docs: Vec<ffi::GidScore> = Vec::with_capacity(limit);
    // A single doc can carry several matching terms (kAllField concatenates all property values),
    // so dedup on the globally-unique gid. `limit` is small, so the set is cheap.
    let mut seen: HashSet<u64> = HashSet::new();

    'segments: for segment_reader in searcher_ctx.searcher.segment_readers() {
        let inverted_index = segment_reader.inverted_index(search_field).map_err(|e| {
            Error::new(ErrorKind::Other, format!("inverted index not available in {:?} -> {}", index_path, e))
        })?;
        let gid_column = segment_reader.fast_fields().u64("gid").map_err(|e| {
            Error::new(ErrorKind::Other, format!("gid fast field not available in {:?} -> {}", index_path, e))
        })?;
        // Postings include soft-deleted docs (Memgraph deletes via delete_query); skip them just as
        // searcher.search() would via the alive bitset.
        let alive_bitset = segment_reader.alive_bitset();

        let mut term_stream = inverted_index.terms().search(regex.as_ref()).into_stream()?;
        while term_stream.advance() {
            let term_info = term_stream.value();
            let mut postings =
                inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            let mut doc = postings.doc();
            while doc != TERMINATED {
                let alive = alive_bitset.map_or(true, |bitset| bitset.is_alive(doc));
                if alive {
                    if let Some(gid) = gid_column.first(doc) {
                        if seen.insert(gid) {
                            // Score matches the constant ConstScorer value RegexQuery would emit.
                            docs.push(ffi::GidScore { gid, score: 1.0 });
                            if docs.len() >= limit {
                                break 'segments;
                            }
                        }
                    }
                }
                doc = postings.advance();
            }
        }
    }
    Ok(ffi::GidScoreOutput { docs })
}

fn search_edge_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::EdgeGidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let mut query_parser = QueryParser::for_index(index, search_fields);
    apply_fuzzy_config(&mut query_parser, schema, input)?;
    let query = query_parser.parse_query(&input.search_query).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Unable to create search query for {:?} -> {}", index_path, e),
        )
    })?;

    let top_docs = searcher_ctx
        .searcher
        .search(&query, &TopDocs::with_limit(input.effective_limit()))
        .map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to perform text search under {:?} -> {}", index_path, e),
            )
        })?;

    // Read the edge/from/to gids from columnar fast fields instead of decompressing the stored
    // document per hit. All three are configured as fast fields.
    let segment_readers = searcher_ctx.searcher.segment_readers();
    let fast_u64_column = |field_name: &str| {
        segment_readers
            .iter()
            .map(|sr| sr.fast_fields().u64(field_name))
            .collect::<Result<Vec<_>, tantivy::TantivyError>>()
            .map_err(|e| {
                Error::new(ErrorKind::Other, format!("{} fast field not available in {:?} -> {}", field_name, index_path, e))
            })
    };
    let edge_gid_columns = fast_u64_column("edge_gid")?;
    let from_gid_columns = fast_u64_column("from_vertex_gid")?;
    let to_gid_columns = fast_u64_column("to_vertex_gid")?;

    let read_gid = |columns: &[tantivy::columnar::Column<u64>], doc: &tantivy::DocAddress, field_name: &str| {
        columns[doc.segment_ord as usize].first(doc.doc_id).ok_or_else(|| {
            Error::new(ErrorKind::Other, format!("{} missing for matched doc in {:?}", field_name, index_path))
        })
    };

    let mut docs: Vec<ffi::EdgeGidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let edge_gid = read_gid(&edge_gid_columns, &doc_address, "edge_gid")?;
        let from_gid = read_gid(&from_gid_columns, &doc_address, "from_vertex_gid")?;
        let to_gid = read_gid(&to_gid_columns, &doc_address, "to_vertex_gid")?;
        docs.push(ffi::EdgeGidScore { edge_gid, from_gid, to_gid, score });
    }
    Ok(ffi::EdgeGidScoreOutput { docs })
}

fn regex_search_edge_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::EdgeGidScoreOutput, std::io::Error> {
    // Same lazy, early-terminating strategy as regex_search_gids_pinned (see comment there): regex
    // scores are constant, so we stream FST-pruned matching terms and stop at `limit` distinct edges
    // instead of materializing the full matching doc set. The edge/from/to gids are read from the
    // columnar fast fields rather than decompressing the stored document per hit.
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;

    let search_field = search_get_fields(&input.search_fields, schema, index_path)?[0];
    let regex = compiled_regex(context, &input.search_query)?;

    let limit = input.effective_limit();
    let mut docs: Vec<ffi::EdgeGidScore> = Vec::with_capacity(limit);
    let mut seen: HashSet<u64> = HashSet::new();

    'segments: for segment_reader in searcher_ctx.searcher.segment_readers() {
        let inverted_index = segment_reader.inverted_index(search_field).map_err(|e| {
            Error::new(ErrorKind::Other, format!("inverted index not available in {:?} -> {}", index_path, e))
        })?;
        let fast_fields = segment_reader.fast_fields();
        let fast_u64_column = |field_name: &str| {
            fast_fields.u64(field_name).map_err(|e| {
                Error::new(ErrorKind::Other, format!("{} fast field not available in {:?} -> {}", field_name, index_path, e))
            })
        };
        let edge_gid_column = fast_u64_column("edge_gid")?;
        let from_gid_column = fast_u64_column("from_vertex_gid")?;
        let to_gid_column = fast_u64_column("to_vertex_gid")?;
        // Postings include soft-deleted docs (Memgraph deletes via delete_query); skip them just as
        // searcher.search() would via the alive bitset.
        let alive_bitset = segment_reader.alive_bitset();

        let mut term_stream = inverted_index.terms().search(regex.as_ref()).into_stream()?;
        while term_stream.advance() {
            let term_info = term_stream.value();
            let mut postings =
                inverted_index.read_postings_from_terminfo(term_info, IndexRecordOption::Basic)?;
            let mut doc = postings.doc();
            while doc != TERMINATED {
                let alive = alive_bitset.map_or(true, |bitset| bitset.is_alive(doc));
                if alive {
                    if let Some(edge_gid) = edge_gid_column.first(doc) {
                        if seen.insert(edge_gid) {
                            let from_gid = from_gid_column.first(doc).ok_or_else(|| {
                                Error::new(ErrorKind::Other, format!("from_vertex_gid missing for matched doc in {:?}", index_path))
                            })?;
                            let to_gid = to_gid_column.first(doc).ok_or_else(|| {
                                Error::new(ErrorKind::Other, format!("to_vertex_gid missing for matched doc in {:?}", index_path))
                            })?;
                            docs.push(ffi::EdgeGidScore { edge_gid, from_gid, to_gid, score: 1.0 });
                            if docs.len() >= limit {
                                break 'segments;
                            }
                        }
                    }
                }
                doc = postings.advance();
            }
        }
    }
    Ok(ffi::EdgeGidScoreOutput { docs })
}

fn aggregate(
    context: &ffi::Context,
    input: &ffi::SearchInput,
) -> Result<ffi::DocumentOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;
    let reader = &context.tantivyContext.index_reader;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let query_parser = QueryParser::for_index(index, search_fields);
    let query = match query_parser.parse_query(&input.search_query) {
        Ok(q) => q,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to create search query {}", e),
            ));
        }
    };
    let searcher = reader.searcher();
    let agg_req: Aggregations = serde_json::from_str(&input.aggregation_query)?;
    let collector = AggregationCollector::from_aggs(agg_req, Default::default());
    let agg_res: AggregationResults = match searcher.search(&query, &collector) {
        Ok(r) => r,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Failed to gather aggregation results for {:?} text search index -> {}",
                    index_path, e
                ),
            ));
        }
    };
    let res: Value = serde_json::to_value(agg_res)?;
    Ok(ffi::DocumentOutput {
        data: res.to_string(),
        score: 0.0, // Aggregation results don't have individual document scores
    })
}

fn get_num_docs(context: &ffi::Context) -> Result<u64, std::io::Error> {
    let reader = &context.tantivyContext.index_reader;
    let searcher = reader.searcher();
    Ok(searcher.num_docs() as u64)
}

/// Drops the index at the given path.
/// This will remove the entire directory and all its contents.
/// NOTE: This function takes ownership of the context.
fn drop_index(context: ffi::Context) -> Result<(), std::io::Error> {
    // Clone (not mem::take) so TantivyContext::drop can still log the path on failure.
    let index_path = context.tantivyContext.index_path.clone();

    // Dropping context triggers TantivyContext::drop, which calls wait_merging_threads
    // before releasing the writer, then drops index_reader and index via field drop.
    drop(context);

    // Now safe to remove the directory
    if index_path.exists() {
        match std::fs::remove_dir_all(&index_path) {
            Ok(_) => {
                debug!("Text search index at {:?} removed", index_path);
            }
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to remove underlying text search index folder -> {}",
                        e
                    ),
                ));
            }
        }
    } else {
        debug!("Index at {:?} does NOT exist", index_path);
    }
    Ok(())
}
