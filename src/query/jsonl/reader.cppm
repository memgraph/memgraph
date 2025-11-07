// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

module;

#include <string>
#include "query/typed_value.hpp"

#include "simdjson.h"

export module memgraph.query.jsonl.reader;

export namespace memgraph::query {
using Row = TypedValue::TMap;

class JsonlReader {
 public:
  explicit JsonlReader(std::string file, std::pmr::memory_resource *resource);
  ~JsonlReader();

  JsonlReader(JsonlReader const &other) = delete;
  JsonlReader &operator=(JsonlReader const &other) = delete;
  JsonlReader(JsonlReader &&other) = default;
  JsonlReader &operator=(JsonlReader &&other) = default;

  auto GetNextRow(Row &out) -> bool;

 private:
  std::string file_;
  std::pmr::memory_resource *resource_;
  simdjson::ondemand::parser parser_;
  simdjson::padded_string content_;
  simdjson::ondemand::document_stream docs_;
  simdjson::ondemand::document_stream::iterator it_;
};

}  // namespace memgraph::query
