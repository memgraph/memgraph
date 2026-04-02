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
use std::io::{Error, ErrorKind};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::AggregationCollector;
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{QueryParser, RegexQuery};
use tantivy::schema::*;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument};

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
        // NOTE: Any primitive value here is a bit of a problem because of default value on the C++
        // side.
    }
    // NOTE: SearchOutput is currently only used by the old search()/regex_search() functions which
    // are not called from C++ anymore (replaced by the *_gids_pinned variants). Keeping it around
    // because we will need it once we add single-store mode to the text index (returning full
    // documents directly from Tantivy instead of doing a separate storage lookup by GID).
    struct SearchOutput {
        docs: Vec<DocumentOutput>,
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
        fn search(context: &mut Context, input: &SearchInput) -> Result<SearchOutput>;
        fn regex_search(context: &mut Context, input: &SearchInput) -> Result<SearchOutput>;
        fn aggregate(context: &mut Context, input: &SearchInput) -> Result<DocumentOutput>;
        fn acquire_searcher(context: &mut Context) -> Result<Box<SearcherContext>>;
        fn search_gids_pinned(
            context: &mut Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<GidScoreOutput>;
        fn regex_search_gids_pinned(
            context: &mut Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<GidScoreOutput>;
        fn search_edge_gids_pinned(
            context: &mut Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<EdgeGidScoreOutput>;
        fn regex_search_edge_gids_pinned(
            context: &mut Context,
            searcher: &SearcherContext,
            input: &SearchInput,
        ) -> Result<EdgeGidScoreOutput>;
        fn get_num_docs(context: &mut Context) -> Result<u64>;
        fn drop_index(context: Context) -> Result<()>;
    }
}

impl ffi::SearchInput {
    fn effective_limit(&self) -> usize {
        if self.limit == 0 { 1000 } else { self.limit }
    }
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
}

pub struct TantivyContext {
    pub index_path: std::path::PathBuf,
    pub schema: Schema,
    pub index: Index,
    pub index_writer: IndexWriter,
    pub index_reader: IndexReader,
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
            index,
            index_writer,
            index_reader,
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
    let index_writer = &mut context.tantivyContext.index_writer;
    match index_writer.add_document(document) {
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
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;
    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let query_parser = QueryParser::for_index(index, search_fields);
    let query = match query_parser.parse_query(&input.search_query) {
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
    };
    let index_writer = &mut context.tantivyContext.index_writer;
    match index_writer.delete_query(query) {
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
                format!(
                    "Unable to delete document from text search index at {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    }
}

fn commit(context: &mut ffi::Context) -> Result<(), std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    match context.tantivyContext.index_writer.commit() {
        Ok(_) => {
            // Explicitly reload the index reader to see the new changes
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
    let index_path = &context.tantivyContext.index_path;
    match context.tantivyContext.index_writer.rollback() {
        Ok(_) => {
            return Ok(());
        }
        Err(e) => {
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

// NOTE: Not currently called from C++ — replaced by search_gids_pinned(). Keeping for future
// single-store text index mode where we return full documents directly from Tantivy.
fn search(
    context: &mut ffi::Context,
    input: &ffi::SearchInput,
) -> Result<ffi::SearchOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;
    let reader = &context.tantivyContext.index_reader;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let return_fields = search_get_fields(&input.return_fields, schema, index_path)?;
    let query_parser = QueryParser::for_index(index, search_fields);
    let query = match query_parser.parse_query(&input.search_query) {
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
    };

    let searcher = reader.searcher();
    let top_docs = match searcher.search(&query, &TopDocs::with_limit(input.effective_limit())) {
        Ok(docs) => docs,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to perform text search under {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    };

    let mut docs: Vec<ffi::DocumentOutput> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = match searcher.doc(doc_address) {
            Ok(d) => d,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Unable to find document inside {:?} text search index) -> {}",
                        index_path, e
                    ),
                ));
            }
        };
        let mut data: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        for (name, field) in input.return_fields.iter().zip(return_fields.iter()) {
            let field_data = match doc.get_first(*field) {
                Some(f) => f,
                None => continue,
            };
            let owned: OwnedValue = field_data.into();
            data.insert(name.to_string(), owned_value_to_json(owned));
        }

        docs.push(ffi::DocumentOutput {
            data: match to_string(&data) {
                Ok(s) => s,
                Err(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!(
                            "Unable to serialize {:?} text search index data into a string -> {}",
                            index_path, e
                        ),
                    ));
                }
            },
            score: score,
        });
    }
    Ok(ffi::SearchOutput { docs })
}

// NOTE: Not currently called from C++ — replaced by regex_search_gids_pinned(). Keeping for
// future single-store text index mode where we return full documents directly from Tantivy.
fn regex_search(
    context: &mut ffi::Context,
    input: &ffi::SearchInput,
) -> Result<ffi::SearchOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;
    let reader = &context.tantivyContext.index_reader;

    let search_field = search_get_fields(&input.search_fields, schema, index_path)?[0];
    let return_fields = search_get_fields(&input.return_fields, schema, index_path)?;

    let query = match RegexQuery::from_pattern(&input.search_query, search_field) {
        Ok(q) => q,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to create regex search query for {:?} text search index -> {}",
                    index_path, e
                ),
            ));
        }
    };

    let searcher = reader.searcher();
    let top_docs = match searcher.search(&query, &TopDocs::with_limit(input.effective_limit())) {
        Ok(docs) => docs,
        Err(e) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "Unable to perform text search under {:?} -> {}",
                    index_path, e
                ),
            ));
        }
    };

    let mut docs: Vec<ffi::DocumentOutput> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = match searcher.doc(doc_address) {
            Ok(d) => d,
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "Unable to find document inside {:?} text search index) -> {}",
                        index_path, e
                    ),
                ));
            }
        };
        let mut data: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        for (name, field) in input.return_fields.iter().zip(return_fields.iter()) {
            let field_data = match doc.get_first(*field) {
                Some(f) => f,
                None => continue,
            };
            let owned: OwnedValue = field_data.into();
            data.insert(name.to_string(), owned_value_to_json(owned));
        }
        docs.push(ffi::DocumentOutput {
            data: match to_string(&data) {
                Ok(s) => s,
                Err(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!(
                            "Unable to serialize {:?} text search index data into a string -> {}",
                            index_path, e
                        ),
                    ));
                }
            },
            score: score,
        });
    }
    Ok(ffi::SearchOutput { docs })
}

fn extract_u64_field(
    doc: &TantivyDocument,
    field: Field,
    field_name: &str,
    index_path: &std::path::PathBuf,
) -> Result<u64, std::io::Error> {
    let value = doc.get_first(field).ok_or_else(|| {
        Error::new(
            ErrorKind::Other,
            format!(
                "Document missing {} field in {:?}",
                field_name, index_path
            ),
        )
    })?;
    let owned: OwnedValue = value.into();
    match owned {
        OwnedValue::U64(n) => Ok(n),
        other => Err(Error::new(
            ErrorKind::Other,
            format!(
                "{} field has unexpected type {:?} in {:?}",
                field_name, other, index_path
            ),
        )),
    }
}

pub struct SearcherContext {
    searcher: tantivy::Searcher,
}

fn acquire_searcher(
    context: &mut ffi::Context,
) -> Result<Box<SearcherContext>, std::io::Error> {
    let searcher = context.tantivyContext.index_reader.searcher();
    Ok(Box::new(SearcherContext { searcher }))
}

fn search_gids_pinned(
    context: &mut ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::GidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let query_parser = QueryParser::for_index(index, search_fields);
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

    let gid_field = schema.get_field("gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("gid field not found in {:?} -> {}", index_path, e))
    })?;

    let mut docs: Vec<ffi::GidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher_ctx.searcher.doc(doc_address).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to find document inside {:?} -> {}", index_path, e),
            )
        })?;
        let gid = extract_u64_field(&doc, gid_field, "gid", index_path)?;
        docs.push(ffi::GidScore { gid, score });
    }
    Ok(ffi::GidScoreOutput { docs })
}

fn regex_search_gids_pinned(
    context: &mut ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::GidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;

    let search_field = search_get_fields(&input.search_fields, schema, index_path)?[0];
    let query = RegexQuery::from_pattern(&input.search_query, search_field).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Unable to create regex search query for {:?} -> {}", index_path, e),
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

    let gid_field = schema.get_field("gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("gid field not found in {:?} -> {}", index_path, e))
    })?;

    let mut docs: Vec<ffi::GidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher_ctx.searcher.doc(doc_address).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to find document inside {:?} -> {}", index_path, e),
            )
        })?;
        let gid = extract_u64_field(&doc, gid_field, "gid", index_path)?;
        docs.push(ffi::GidScore { gid, score });
    }
    Ok(ffi::GidScoreOutput { docs })
}

fn search_edge_gids_pinned(
    context: &mut ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::EdgeGidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let index = &context.tantivyContext.index;
    let schema = &context.tantivyContext.schema;

    let search_fields = search_get_fields(&input.search_fields, schema, index_path)?;
    let query_parser = QueryParser::for_index(index, search_fields);
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

    let edge_gid_field = schema.get_field("edge_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("edge_gid not found in {:?} -> {}", index_path, e))
    })?;
    let from_gid_field = schema.get_field("from_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("from_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;
    let to_gid_field = schema.get_field("to_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("to_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;

    let mut docs: Vec<ffi::EdgeGidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher_ctx.searcher.doc(doc_address).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to find document inside {:?} -> {}", index_path, e),
            )
        })?;
        let edge_gid = extract_u64_field(&doc, edge_gid_field, "edge_gid", index_path)?;
        let from_gid = extract_u64_field(&doc, from_gid_field, "from_vertex_gid", index_path)?;
        let to_gid = extract_u64_field(&doc, to_gid_field, "to_vertex_gid", index_path)?;
        docs.push(ffi::EdgeGidScore { edge_gid, from_gid, to_gid, score });
    }
    Ok(ffi::EdgeGidScoreOutput { docs })
}

fn regex_search_edge_gids_pinned(
    context: &mut ffi::Context,
    searcher_ctx: &SearcherContext,
    input: &ffi::SearchInput,
) -> Result<ffi::EdgeGidScoreOutput, std::io::Error> {
    let index_path = &context.tantivyContext.index_path;
    let schema = &context.tantivyContext.schema;

    let search_field = search_get_fields(&input.search_fields, schema, index_path)?[0];
    let query = RegexQuery::from_pattern(&input.search_query, search_field).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("Unable to create regex search query for {:?} -> {}", index_path, e),
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

    let edge_gid_field = schema.get_field("edge_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("edge_gid not found in {:?} -> {}", index_path, e))
    })?;
    let from_gid_field = schema.get_field("from_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("from_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;
    let to_gid_field = schema.get_field("to_vertex_gid").map_err(|e| {
        Error::new(ErrorKind::Other, format!("to_vertex_gid not found in {:?} -> {}", index_path, e))
    })?;

    let mut docs: Vec<ffi::EdgeGidScore> = Vec::with_capacity(top_docs.len());
    for (score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher_ctx.searcher.doc(doc_address).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Unable to find document inside {:?} -> {}", index_path, e),
            )
        })?;
        let edge_gid = extract_u64_field(&doc, edge_gid_field, "edge_gid", index_path)?;
        let from_gid = extract_u64_field(&doc, from_gid_field, "from_vertex_gid", index_path)?;
        let to_gid = extract_u64_field(&doc, to_gid_field, "to_vertex_gid", index_path)?;
        docs.push(ffi::EdgeGidScore { edge_gid, from_gid, to_gid, score });
    }
    Ok(ffi::EdgeGidScoreOutput { docs })
}

fn aggregate(
    context: &mut ffi::Context,
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

fn get_num_docs(context: &mut ffi::Context) -> Result<u64, std::io::Error> {
    let reader = &context.tantivyContext.index_reader;
    let searcher = reader.searcher();
    Ok(searcher.num_docs() as u64)
}

/// Drops the index at the given path.
/// This will remove the entire directory and all its contents.
/// NOTE: This function takes ownership of the context.
fn drop_index(context: ffi::Context) -> Result<(), std::io::Error> {
    let TantivyContext {
        index_path,
        schema: _,
        index,
        index_writer,
        index_reader,
    } = *context.tantivyContext;

    // Drop Tantivy resources to release file handles
    drop(index_reader);
    drop(index_writer);
    drop(index);

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
