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
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

// NOTE: Result<T> == Result<T,std::io::Error>.
#[cxx::bridge(namespace = "mgcxx::text_search")]
mod ffi {
    // TODO(gitbuda): Try to put direct pointers to the tantivy datastructures under Context
    struct Context {
        tantivyContext: Box<TantivyContext>,
    }

    // TODO(gitbuda): Write all the right combinations.
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
        // TODO(gitbuda): Add stuff like skip.
        // NOTE: Any primitive value here is a bit of a problem because of default value on the C++
        // side.
    }
    struct SearchOutput {
        docs: Vec<DocumentOutput>,
        // TODO(gitbuda): Add stuff like page (skip, limit).
    }

    // NOTE: Since return type is Result<T>, always return Result<Something>.
    extern "Rust" {
        type TantivyContext;
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
        fn search(context: &mut Context, input: &SearchInput) -> Result<SearchOutput>;
        fn regex_search(context: &mut Context, input: &SearchInput) -> Result<SearchOutput>;
        fn aggregate(context: &mut Context, input: &SearchInput) -> Result<DocumentOutput>;
        fn get_num_docs(context: &mut Context) -> Result<u64>;
        fn drop_index(context: Context) -> Result<()>;
    }
}

impl ffi::SearchInput {
    fn effective_limit(&self) -> usize {
        if self.limit == 0 { 1000 } else { self.limit }
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
    // TODO(gitbuda): Used as a library code inside a C++ application -> align logger format.
    let log_init_res = env_logger::try_init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "warn"),
    );
    // TODO(gitbuda): If more than one module tries to do this -> the later call might fail ->
    // in that case, this code should be adjusted (or the error should be ignored because the
    // logger is already initialized) -> if this happens consider what would be the best solution.
    if let Err(e) = log_init_res {
        return Err(Error::new(
                ErrorKind::Other,
                format!("Unable to initialize tantivy (text search engine) logger -> {} -> you should probably stop your entire process and make sure it can be initialized properly.", e),
        ));
    }
    Ok(())
}

// TODO(gitbuda): Implement full range of extract_schema options.
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
            // TODO(gitbuda): Shouldn't not just be JSON -> deduce from mappings!
            let field_as_tantivy_json = match field_data {
                OwnedValue::Object(f) => f,
                _ => {
                    // TODO(gitbuda): Is error here the best?
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unable to convert field data to json"),
                    ));
                }
            };
            let field_as_json = match serde_json::to_value(field_as_tantivy_json) {
                Ok(f) => f,
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unable to convert field data to json"),
                    ));
                }
            };
            data.insert(name.to_string(), field_as_json);
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
            // TODO(gitbuda): Shouldn't not just be JSON -> deduce from mappings!
            let field_as_tantivy_json = match field_data {
                OwnedValue::Object(f) => f,
                _ => {
                    // TODO(gitbuda): Is error here the best?
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unable to convert field data to json. Data we have: {:?}", field_data),
                    ));
                }
            };
            let field_as_json = match serde_json::to_value(field_as_tantivy_json) {
                Ok(f) => f,
                Err(_) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("Unable to convert field data to json"),
                    ));
                }
            };
            data.insert(name.to_string(), field_as_json);
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
