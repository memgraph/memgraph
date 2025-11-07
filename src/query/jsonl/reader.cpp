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
#include <utility>

#include "simdjson.h"
#include "spdlog/spdlog.h"

#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"

module memgraph.query.jsonl.reader;

// TODO: (andi) It would probably be better that we create only once parser and reuse it for all documents and
// subsequent calls
// TODO: (andi) Type system?
// TODO: (andi) Performance improvements

namespace {
using memgraph::query::TypedValue;
using simdjson::ondemand::json_type;
using simdjson::ondemand::number_type;

auto ToTypedValue(simdjson::ondemand::value &val, memgraph::utils::MemoryResource *resource) -> TypedValue {
  switch (val.type()) {
    case json_type::null: {
      return TypedValue{resource};
    }
    case json_type::boolean: {
      return TypedValue{val.get_bool(), resource};
    }
    case json_type::number: {
      auto number = val.get_number();
      if (number->is_double()) {
        return TypedValue{number->get_double(), resource};
      }
      if (number->is_int64()) {
        spdlog::trace("Read int64: {}", std::to_string(val.get_int64()));
        return TypedValue{number->get_int64(), resource};
      }
      if (number->is_uint64()) {
        return TypedValue{static_cast<int64_t>(number->get_uint64()), resource};
      }
      // If it is neither of these, then it must be a big integer which needs to be parsed as string
      return TypedValue{val.get_raw_json_string()->raw(), resource};
    }
    case json_type::string: {
      spdlog::trace("Read value str: {}", val.get_string().value());
      return TypedValue{val.get_string().value(), resource};
    }
    default: {
      spdlog::trace("Unknown type");
      return TypedValue{};
    }
      // TODO: (andi) Use std::unreachable
  }
}

}  // namespace

namespace memgraph::query {
JsonlReader::JsonlReader(std::string file, std::pmr::memory_resource *resource)
    : file_{std::move(file)}, resource_{resource} {
  // Load file
  auto jsonl = simdjson::padded_string::load(file_);
  if (!jsonl.has_value()) {
    throw utils::BasicException("Failed to load file {}.", file_);
  }
  content_ = std::move(jsonl.value());
  // Create docs iterator
  auto error = parser_.iterate_many(content_).get(docs_);
  if (error) {
    throw utils::BasicException("Failed to create iterator over documents for file {}", file_);
  }

  it_ = docs_.begin();
  spdlog::trace("Cached iterator");
}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool {
  if (it_ == docs_.end()) return false;

  if ((*it_).error()) {
    spdlog::error("Failed to parse document: {}", simdjson::error_message((*it_).error()));
    ++it_;
    return GetNextRow(out);
  }

  for (auto field : (*it_)->get_object()) {
    std::string_view key_view;
    auto error = field->unescaped_key().get(key_view);
    if (error) continue;

    TypedValue::TString key{key_view, resource_};
    auto maybe_val = field->value();

    auto val = ToTypedValue(maybe_val, resource_);
  }

  ++it_;
  return true;
}
}  // namespace memgraph::query
