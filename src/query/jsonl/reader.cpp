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

#include "simdjson.h"
#include "spdlog/spdlog.h"

#include "utils/exceptions.hpp"

module memgraph.query.jsonl.reader;

// TODO: (andi) It would probably be better that we create only once JsonlReader and reuse it for all documents and
// subsequent calls

namespace memgraph::query {
JsonlReader::JsonlReader(std::string file, std::pmr::memory_resource *resource)
    : file_{std::move(file)}, resource_{resource} {
  // Load file
  auto jsonl = simdjson::padded_string::load(file_);
  if (!jsonl.has_value()) {
    throw utils::BasicException("Failed to load file {}.", file_);
  }
  jsonl_ = std::move(jsonl.value());
  // Create docs iterator
  auto error = parser_.iterate_many(jsonl_).get(docs_);
  if (error) {
    throw utils::BasicException("Failed to create iterator over documents for file {}", file_);
  }

  it_ = docs_.begin();
  spdlog::trace("Cached iterator");
}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool {
  if (it_ == docs_.end()) return false;
  // Access raw JSON immediately (don't store doc)
  std::string_view json = (*it_)->raw_json().value();
  spdlog::trace("Doc: {}", json);

  ++it_;
  return true;
}
}  // namespace memgraph::query
