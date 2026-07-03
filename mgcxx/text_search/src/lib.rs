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
use serde_json::Value;
use std::collections::HashSet;
use std::io::{Error, ErrorKind};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::AggregationCollector;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, QueryParser, RegexQuery};
use tantivy::schema::Value as _;
use tantivy::schema::*;
use tantivy::tokenizer::{TextAnalyzer, TokenStream};
use tantivy::{DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term, TERMINATED};
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
        // Fuzzy phrase search: ordered, adjacent terms, last term a prefix. search_query is
        // `data.<property>:<terms>`; fuzzy_distance/transpositions carry the user's exact values.
        fn fuzzy_phrase_search_gids_pinned(
            context: &Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<GidScoreOutput>;
        fn fuzzy_phrase_search_edge_gids_pinned(
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

    fn chars(s: &str) -> Vec<char> {
        s.chars().collect()
    }
    fn toks(words: &[&str]) -> Vec<(usize, Vec<char>)> {
        words.iter().enumerate().map(|(position, w)| (position, chars(w))).collect()
    }

    #[test]
    fn split_property_and_terms_accepts_single_property_form() {
        assert_eq!(
            split_property_and_terms("data.title:big bad wo").unwrap(),
            ("title".to_string(), "big bad wo".to_string())
        );
        assert_eq!(
            split_property_and_terms("data.a.b:x").unwrap(),
            ("a.b".to_string(), "x".to_string())
        );
        assert_eq!(
            split_property_and_terms("  data.title :big bad wo").unwrap(),
            ("title".to_string(), "big bad wo".to_string())
        );
    }

    #[test]
    fn split_property_and_terms_rejects_other_forms() {
        for q in ["big bad wo", "all:foo", "data.:foo", "data. :foo", "nocolon"] {
            assert!(split_property_and_terms(q).is_err(), "expected error for {:?}", q);
        }
    }

    #[test]
    fn word_and_prefix_distance() {
        assert_eq!(word_distance(&chars("bad"), &chars("bad"), true), 0);
        assert_eq!(word_distance(&chars("bd"), &chars("bad"), true), 1); // one insertion
        assert_eq!(word_distance(&chars("ba"), &chars("ab"), true), 1); // transposition
        assert_eq!(word_distance(&chars("ba"), &chars("ab"), false), 2); // without transposition
        assert_eq!(prefix_distance(&chars("wo"), &chars("wolf"), true), 0); // wo prefixes wolf
        assert_eq!(prefix_distance(&chars("wolf"), &chars("wolf"), true), 0);
        assert_eq!(prefix_distance(&chars("wolf"), &chars("world"), true), 2); // wolf is not a prefix
        assert_eq!(prefix_distance(&chars("xo"), &chars("wolf"), true), 1); // wo within 1 of xo
    }

    #[test]
    fn fuzzy_phrase_matches_enforces_order_adjacency_and_prefix() {
        let value = toks(&["big", "bad", "wolf"]);
        assert!(fuzzy_phrase_matches(&value, &toks(&["big", "bad", "wo"]), 0, true));
        assert!(!fuzzy_phrase_matches(&value, &toks(&["bad", "big", "wo"]), 0, true)); // order
        assert!(!fuzzy_phrase_matches(&value, &toks(&["big", "wolf"]), 0, true)); // adjacency
        assert!(fuzzy_phrase_matches(&toks(&["the", "big", "bad", "wolf"]), &toks(&["big", "bad", "wo"]), 0, true));
        let world = toks(&["big", "bad", "world"]);
        assert!(fuzzy_phrase_matches(&world, &toks(&["big", "bad", "wo"]), 0, true)); // wo prefixes world
        assert!(!fuzzy_phrase_matches(&world, &toks(&["big", "bad", "wolf"]), 0, true)); // wolf does not
        assert!(!fuzzy_phrase_matches(&toks(&["big"]), &toks(&["big", "bad"]), 0, true)); // too few tokens
    }

    #[test]
    fn fuzzy_phrase_matches_budget_is_per_whole_input_not_per_term() {
        let value = toks(&["big", "bad", "wolf"]);
        assert!(fuzzy_phrase_matches(&value, &toks(&["big", "bd", "wo"]), 1, true)); // 1 total edit
        assert!(!fuzzy_phrase_matches(&value, &toks(&["big", "bd", "wo"]), 0, true));
        // two single-term typos = 2 total edits: rejected at distance 1 (would pass a per-term budget),
        // accepted at distance 2.
        assert!(!fuzzy_phrase_matches(&value, &toks(&["bg", "bd", "wo"]), 1, true));
        assert!(fuzzy_phrase_matches(&value, &toks(&["bg", "bd", "wo"]), 2, true));
        // transposition counts as one edit only when enabled.
        assert!(fuzzy_phrase_matches(&value, &toks(&["big", "abd", "wo"]), 1, true));
        assert!(!fuzzy_phrase_matches(&value, &toks(&["big", "abd", "wo"]), 1, false));
        // the shared budget is NOT spent on word boundaries: words stay aligned to tokens, so a query
        // does not fuzzy-merge across a space. This keeps the post-filter a subset of the candidate net.
        assert!(!fuzzy_phrase_matches(&toks(&["newyork", "city"]), &toks(&["new", "york"]), 1, true));
    }

    #[test]
    fn fuzzy_phrase_matches_respects_position_gaps_from_dropped_tokens() {
        let mut analyzer = tantivy::tokenizer::TokenizerManager::default().get("default").unwrap();
        let blob = "x".repeat(45);
        let gapped_value = analyzer_tokenize(&mut analyzer, &format!("big {blob} bad wolf"));
        let terms = analyzer_tokenize(&mut analyzer, "big bad wo");
        assert!(!fuzzy_phrase_matches(&gapped_value, &terms, 0, true));
        assert!(!fuzzy_phrase_matches(&gapped_value, &terms, 2, true));
        let gapped_terms = analyzer_tokenize(&mut analyzer, &format!("big {blob} bad wo"));
        assert!(fuzzy_phrase_matches(&gapped_value, &gapped_terms, 0, true));
        assert!(!fuzzy_phrase_matches(&analyzer_tokenize(&mut analyzer, "big bad wolf"), &gapped_terms, 0, true));
    }

    #[test]
    fn analyzer_tokenize_matches_default_tokenizer() {
        let mut analyzer = tantivy::tokenizer::TokenizerManager::default().get("default").unwrap();
        assert_eq!(analyzer_tokenize(&mut analyzer, "Big Bad Wolf"), toks(&["big", "bad", "wolf"]));
        // Unicode is segmented and lowercased correctly (the previous ASCII tokenizer would split this).
        assert_eq!(analyzer_tokenize(&mut analyzer, "Café"), toks(&["café"]));
        // RemoveLongFilter drops tokens of 40+ bytes.
        let long = "a".repeat(40);
        assert!(analyzer_tokenize(&mut analyzer, &long).is_empty());
        assert_eq!(analyzer_tokenize(&mut analyzer, &"a".repeat(39)).len(), 1);
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

    // Adaptive candidate paging: the 10 real matches sit between 150 decoys on each side, so the first
    // page (limit 10 * FUZZY_PHRASE_OVERFETCH_FACTOR = 80 candidates) holds only decoys whichever way score
    // ties break -- reaching the matches requires widening past the first page via and_offset. The
    // decoys carry every query term in the wrong order, so the post-filter must reject them across
    // pages; returning exactly the 10 proves no boundary drop or duplicate.
    #[test]
    fn fuzzy_phrase_search_pages_past_the_first_candidate_page() {
        let dir = std::env::temp_dir().join(format!("mgcxx_seq_paging_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.to_str().unwrap().to_string();
        let mappings = r#"{"properties":{
            "data":{"type":"json","fast":true,"stored":true,"text":true},
            "all":{"type":"text","fast":true,"stored":true,"text":true},
            "gid":{"type":"u64","fast":true,"stored":true,"indexed":true}}}"#
            .to_string();
        let mut ctx = create_index(&path, &ffi::IndexConfig { mappings }).unwrap();

        let mut add_doc = |gid: u64, name: &str| {
            let data = format!(r#"{{"data":{{"name":"{name}"}},"all":"{name}","gid":{gid}}}"#);
            add_document(&mut ctx, &ffi::DocumentInput { data }, true).unwrap();
        };
        for gid in 1u64..=150 {
            add_doc(gid, "wolf bad big");
        }
        let real: Vec<u64> = (1000..1010).collect();
        for &gid in &real {
            add_doc(gid, "big bad wolf");
        }
        for gid in 2000u64..=2150 {
            add_doc(gid, "wolf bad big");
        }
        commit(&mut ctx).unwrap();

        let searcher = acquire_searcher(&ctx).unwrap();
        let input = ffi::SearchInput {
            search_fields: vec![],
            search_query: "data.name:big bad wo".to_string(),
            return_fields: vec![],
            aggregation_query: String::new(),
            limit: 10,
            fuzzy_distance: 0,
            fuzzy_prefix: true,
            fuzzy_transpositions: false,
            fuzzy_field: String::new(),
        };
        let mut gids: Vec<u64> = fuzzy_phrase_search_gids_pinned(&ctx, &searcher, &input)
            .unwrap()
            .docs
            .iter()
            .map(|d| d.gid)
            .collect();
        gids.sort_unstable();
        assert_eq!(gids, real);

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

// Adaptive candidate paging for fuzzy_phrase_search. The post-filter discards candidates, so the first page
// over-fetches the effective limit by FUZZY_PHRASE_OVERFETCH_FACTOR and doubles each round when starved.
// FUZZY_PHRASE_MAX_CANDIDATES caps how many score-ranked candidates one query will ever scan.
// (Future scalability lever: a str/bytes fast field + columnar collector would filter without the store.)
const FUZZY_PHRASE_OVERFETCH_FACTOR: usize = 8;
const FUZZY_PHRASE_MAX_CANDIDATES: usize = 50_000;

// Parses `data.<property>:<terms>` into (property, terms); the field side is trimmed. fuzzy_phrase_search
// is single-property only, so any other form (no colon, non-`data` prefix, or blank property) is rejected.
fn split_property_and_terms(query: &str) -> Result<(String, String), std::io::Error> {
    query
        .split_once(':')
        .and_then(|(lhs, terms)| {
            let property = lhs.trim().strip_prefix("data.")?.trim();
            (!property.is_empty()).then(|| (property.to_string(), terms.to_string()))
        })
        .ok_or_else(|| {
            Error::new(ErrorKind::Other, format!("fuzzy_phrase_search expects 'data.<property>:<terms>', got: {}", query))
        })
}

// Runs the field's own analyzer over `text`, yielding the exact (position, token) pairs tantivy
// indexed (Unicode segmentation, RemoveLongFilter, lowercasing -- no approximation).
fn analyzer_tokenize(analyzer: &mut TextAnalyzer, text: &str) -> Vec<(usize, Vec<char>)> {
    let mut tokens = Vec::new();
    let mut stream = analyzer.token_stream(text);
    while stream.advance() {
        let token = stream.token();
        tokens.push((token.position, token.text.chars().collect()));
    }
    tokens
}

// Optimal-string-alignment DP over chars. Returns the final row M[term.len()][*], so the full edit
// distance is row[token.len()] and the min over prefixes of `token` is min(row).
fn edit_row(term: &[char], token: &[char], transpositions: bool) -> Vec<usize> {
    let lk = token.len();
    let mut transposition_row = vec![0usize; lk + 1];
    let mut prev_row = (0..=lk).collect::<Vec<_>>();
    let mut curr_row = vec![0usize; lk + 1];
    for i in 1..=term.len() {
        curr_row[0] = i;
        for j in 1..=lk {
            let cost = (term[i - 1] != token[j - 1]) as usize;
            let mut best = (prev_row[j] + 1).min(curr_row[j - 1] + 1).min(prev_row[j - 1] + cost);
            if transpositions && i > 1 && j > 1 && term[i - 1] == token[j - 2] && term[i - 2] == token[j - 1] {
                best = best.min(transposition_row[j - 2] + 1);
            }
            curr_row[j] = best;
        }
        std::mem::swap(&mut transposition_row, &mut prev_row);
        std::mem::swap(&mut prev_row, &mut curr_row);
    }
    prev_row
}

// Whole-word edit distance between `term` and the full `token` (the final DP cell).
fn word_distance(term: &[char], token: &[char], transpositions: bool) -> usize {
    *edit_row(term, token, transpositions).last().expect("row has token.len()+1 cells")
}

// Smallest edit distance between `term` and any prefix of `token` (min over the DP row).
fn prefix_distance(term: &[char], token: &[char], transpositions: bool) -> usize {
    edit_row(term, token, transpositions).into_iter().min().expect("row is non-empty")
}

// True if `terms` appear as an adjacent, in-order window in `value` (leading terms whole-word, last
// term a prefix). The per-word distances share one budget: their sum must stay within `distance`.
fn fuzzy_phrase_matches(
    value: &[(usize, Vec<char>)],
    terms: &[(usize, Vec<char>)],
    distance: u8,
    transpositions: bool,
) -> bool {
    let n = terms.len();
    if n == 0 || value.len() < n {
        return false;
    }
    let last = n - 1;
    let budget = distance as usize;
    (0..=value.len() - n).any(|start| {
        let mut total = 0usize;
        for (j, (term_position, term)) in terms.iter().enumerate() {
            let (token_position, token) = &value[start + j];
            if token_position - value[start].0 != term_position - terms[0].0 {
                return false;
            }
            total += if j == last {
                prefix_distance(term, token, transpositions)
            } else {
                word_distance(term, token, transpositions)
            };
            if total > budget {
                return false;
            }
        }
        true
    })
}

// Reads the string value of `property` from a hit's stored `data` JSON object, if present.
fn extract_property_string(doc: &TantivyDocument, data_field: Field, property: &str) -> Option<String> {
    doc.get_first(data_field)?
        .as_object()?
        .find(|(key, _)| *key == property)
        .and_then(|(_, value)| value.as_str().map(str::to_string))
}

// Reads a stored u64 field (gid / edge gids) from a fuzzy-phrase-search hit's document.
fn extract_u64_field(
    doc: &TantivyDocument,
    field: Field,
    field_name: &str,
    index_path: &std::path::PathBuf,
) -> Result<u64, std::io::Error> {
    doc.get_first(field).and_then(|value| value.as_u64()).ok_or_else(|| {
        Error::new(
            ErrorKind::Other,
            format!("Document missing u64 {} field in {:?}", field_name, index_path),
        )
    })
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
    if let Some(regex) = context.tantivyContext.regex_cache.lock().unwrap_or_else(|p| p.into_inner()).get(pattern) {
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
        .unwrap_or_else(|p| p.into_inner())
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
    // so dedup on the globally-unique gid.
    let mut seen: HashSet<u64> = HashSet::with_capacity(limit);

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

// Runs the candidate query for a fuzzy phrase search and post-filters to the docs whose
// `data.<property>` value contains the phrase (adjacent, in order, last term a prefix), up to the
// result limit.
fn fuzzy_phrase_matched_docs(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<Vec<(f32, TantivyDocument)>, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;

    let (property, terms_text) = split_property_and_terms(&input.search_query)?;
    let data_field = schema.get_field("data").map_err(|e| {
        Error::new(ErrorKind::Other, format!("data field not found in {:?} -> {}", index_path, e))
    })?;
    let mut analyzer = index.tokenizer_for_field(data_field).map_err(|e| {
        Error::new(ErrorKind::Other, format!("no tokenizer for data field in {:?} -> {}", index_path, e))
    })?;
    let terms = analyzer_tokenize(&mut analyzer, &terms_text);
    if terms.is_empty() {
        return Err(Error::new(
            ErrorKind::Other,
            "fuzzy_phrase_search query produced no searchable terms.".to_string(),
        ));
    }

    // Candidate net built directly (not via the query parser) so numeric/boolean-looking tokens match
    // as string terms rather than failing tantivy's JSON fast-value conversion. Every term is required:
    // last a fuzzy prefix, leading ones whole-word fuzzy. A genuine superset of the post-filter below
    // (a window whose summed distances are <= d has every term individually <= d).
    let last = terms.len() - 1;
    let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
        .iter()
        .enumerate()
        .map(|(i, (_, token))| {
            let mut term = Term::from_field_json_path(data_field, &property, false);
            term.append_type_and_str(&token.iter().collect::<String>());
            let fuzzy = if i == last {
                FuzzyTermQuery::new_prefix(term, input.fuzzy_distance, input.fuzzy_transpositions)
            } else {
                FuzzyTermQuery::new(term, input.fuzzy_distance, input.fuzzy_transpositions)
            };
            (Occur::Must, Box::new(fuzzy) as Box<dyn Query>)
        })
        .collect();
    let query = BooleanQuery::new(subqueries);

    // Page through score-ranked candidates, post-filtering each page, until we've collected
    // `result_limit` matches or the candidate stream runs out. Selective queries fill the first page;
    // we widen only when the post-filter is starved, capped at FUZZY_PHRASE_MAX_CANDIDATES.
    let result_limit = input.effective_limit();
    let mut matched: Vec<(f32, TantivyDocument)> = Vec::new();
    let mut scanned = 0usize;
    let mut page = result_limit.saturating_mul(FUZZY_PHRASE_OVERFETCH_FACTOR).min(FUZZY_PHRASE_MAX_CANDIDATES);
    while matched.len() < result_limit && scanned < FUZZY_PHRASE_MAX_CANDIDATES {
        let page_limit = page.min(FUZZY_PHRASE_MAX_CANDIDATES - scanned);
        let top_docs = searcher_ctx
            .searcher
            .search(&query, &TopDocs::with_limit(page_limit).and_offset(scanned))
            .map_err(|e| {
                Error::new(ErrorKind::Other, format!("Unable to perform fuzzy phrase search under {:?} -> {}", index_path, e))
            })?;
        let fetched = top_docs.len();
        for (score, doc_address) in top_docs {
            let doc: TantivyDocument = searcher_ctx.searcher.doc(doc_address).map_err(|e| {
                Error::new(ErrorKind::Other, format!("Unable to find document inside {:?} -> {}", index_path, e))
            })?;
            let Some(value) = extract_property_string(&doc, data_field, &property) else {
                continue;
            };
            let value_tokens = analyzer_tokenize(&mut analyzer, &value);
            if fuzzy_phrase_matches(&value_tokens, &terms, input.fuzzy_distance, input.fuzzy_transpositions) {
                matched.push((score, doc));
                if matched.len() >= result_limit {
                    break;
                }
            }
        }
        scanned += fetched;
        if fetched < page_limit {
            break; // candidate stream exhausted -- no more docs to page into
        }
        page = page.saturating_mul(2);
    }
    if matched.len() < result_limit && scanned >= FUZZY_PHRASE_MAX_CANDIDATES {
        log::warn!(
            "fuzzy_phrase_search hit the {} candidate scan cap in {:?}; results may be truncated",
            FUZZY_PHRASE_MAX_CANDIDATES,
            index_path
        );
    }
    Ok(matched)
}

fn fuzzy_phrase_search_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::GidScoreOutput, std::io::Error> {
    let matched = fuzzy_phrase_matched_docs(context, searcher_ctx, input)?;
    let index_path = &context.tantivyContext.index_path;
    let gid_field = context.tantivyContext.schema.get_field("gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("gid field not found in {:?} -> {}", index_path, e))
    })?;
    let mut docs: Vec<ffi::GidScore> = Vec::with_capacity(matched.len());
    for (score, doc) in matched {
        let gid = extract_u64_field(&doc, gid_field, "gid", index_path)?;
        docs.push(ffi::GidScore { gid, score });
    }
    Ok(ffi::GidScoreOutput { docs })
}

fn fuzzy_phrase_search_edge_gids_pinned(
    context: &ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::EdgeGidScoreOutput, std::io::Error> {
    let matched = fuzzy_phrase_matched_docs(context, searcher_ctx, input)?;
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;
    let edge_gid_field = schema.get_field("edge_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("edge_gid not found in {:?} -> {}", index_path, e))
    })?;
    let from_gid_field = schema.get_field("from_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("from_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;
    let to_gid_field = schema.get_field("to_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("to_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;
    let mut docs: Vec<ffi::EdgeGidScore> = Vec::with_capacity(matched.len());
    for (score, doc) in matched {
        let edge_gid = extract_u64_field(&doc, edge_gid_field, "edge_gid", index_path)?;
        let from_gid = extract_u64_field(&doc, from_gid_field, "from_vertex_gid", index_path)?;
        let to_gid = extract_u64_field(&doc, to_gid_field, "to_vertex_gid", index_path)?;
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
    let mut seen: HashSet<u64> = HashSet::with_capacity(limit);

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
