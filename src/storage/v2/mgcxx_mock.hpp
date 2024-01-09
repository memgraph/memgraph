// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string>

#ifndef MEMCXX
#define MEMCXX

namespace mgcxx_mock {
namespace text_search {
struct IndexContext {
  std::string tantivyContext;  // the actual type of tantivyContext is outside the mgcxx API
};

struct IndexConfig {
  std::string mappings;
};

struct DocumentInput {
  std::string data;
};

struct DocumentOutput {
  std::string data;
};

struct SearchInput {
  std::vector<std::string> search_fields;
  std::string search_query;
  std::vector<std::string> return_fields;
  std::string aggregation_query;
};

struct SearchOutput {
  std::vector<DocumentOutput> docs;
};

// NOTE:
// * The function names don't follow the style guide in order to be uniform with the mgcxx API
// * All methods are static in order to avoid having to make a Mock object that's globally available
class Mock {
 public:
  static void init(std::string _log_level) {}

  static IndexContext create_index(std::string path, IndexConfig config) { return IndexContext(); }

  static void add(IndexContext context, DocumentInput input, bool skip_commit) {}

  static void delete_document(IndexContext context, SearchInput input, bool skip_commit) {}

  static void commit(IndexContext context) {}

  static void rollback(IndexContext context) {}

  static SearchOutput search(IndexContext context, SearchInput input) {
    return SearchOutput{.docs = {DocumentOutput{.data = "0"}}};
  }

  static DocumentOutput aggregate(IndexContext context, SearchInput input) { return DocumentOutput(); }

  static void drop_index(std::string path) {}
};
}  // namespace text_search
}  // namespace mgcxx_mock

#endif
