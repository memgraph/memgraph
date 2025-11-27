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

#include <functional>
#include <string>

#include "query/typed_value.hpp"

export module memgraph.query.jsonl.reader;

export namespace memgraph::query {
using Row = TypedValue::TMap;

class JsonlReader {
 public:
  explicit JsonlReader(std::string file, std::pmr::memory_resource *resource, std::function<void()> abort_check);
  ~JsonlReader();

  JsonlReader(JsonlReader const &other) = delete;
  JsonlReader &operator=(JsonlReader const &other) = delete;
  JsonlReader(JsonlReader &&other) = default;
  JsonlReader &operator=(JsonlReader &&other) = default;

  auto GetNextRow(Row &out) -> bool;

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;
};

}  // namespace memgraph::query
