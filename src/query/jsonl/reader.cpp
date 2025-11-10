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
#include "utils/likely.hpp"

module memgraph.query.jsonl.reader;

// TODO: (andi) It would probably be better that we create only once parser anulnd reuse it for all documents and
// subsequent calls
// TODO: (andi) Type system?
// TODO: (andi) Performance improvements
// TODO: (andi) Handle all .value scenarios safely
// TODO: (andi) Handle map and array
// TODO: (andi) How to handle uint64_t type here and in LOAD PARQUET clause, ask in the team core?
// TODO: (andi) Use UNLIKELY for errors
// TODO: (andi) If you are using .error() to check for error I think .value_unsafe() should be used because .value()
// does the error checking
// TODO: (andi) load parquet maybe has the issue with swapping if not whole file is under the same schema
// TODO: (andi) Test specific overflow/underflow values

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
      auto num_type = val.get_number_type();
      switch (num_type.value()) {
        case number_type::floating_point_number: {
          return TypedValue{val.get_double(), resource};
        }
        case number_type::signed_integer: {
          return TypedValue{val.get_int64(), resource};
        }
        case number_type::unsigned_integer: {
          // NOTE: uint64_t read as int64_t
          return TypedValue{static_cast<int64_t>(val.get_uint64()), resource};
        }
        case number_type::big_integer: {
          // NOTE: big integer read as raw json
          return TypedValue{val.raw_json_token(), resource};
        }
        default: {
          std::unreachable();
        }
      }
    }
    case json_type::string: {
      return TypedValue{val.get_string().value(), resource};
    }
    case json_type::array: {
      TypedValue::TVector t_vec;
      auto arr = val.get_array();
      if (UNLIKELY(arr.error())) {
        spdlog::error("Error when reading JSONL array. Null value will be used to represent the whole array.");
        return TypedValue{resource};
      }

      for (auto it = arr->begin(); it != arr->end(); ++it) {
        if (UNLIKELY(it.error())) {
          spdlog::error(
              "Error when reading element in JSONL array. Null value will be used to represent the element in the "
              "array.");
          // TODO: (andi) Try to use emplace_back without creating temporary but I rememeber I had some issues with
          // parquet file
          t_vec.push_back(TypedValue{resource});
        }
        auto arr_elem_value = (*it).value_unsafe();
        t_vec.push_back(ToTypedValue(arr_elem_value, resource));
      }

      return TypedValue{std::move(t_vec), resource};
    }
    case json_type::object: {
      return TypedValue{resource};
    }
    case json_type::unknown: {
      spdlog::trace(
          "Found bad token in the JSON document. Null value will be used instead of this token. The rest of the "
          "document will be processed normally.");
      return TypedValue{resource};
    }
    default: {
      std::unreachable();
    }
  }
}

}  // namespace

namespace memgraph::query {

struct JsonlReader::impl {
 public:
  impl(std::string file, std::pmr::memory_resource *resource) : file_{std::move(file)}, resource_{resource} {
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
  }

  auto GetNextRow(Row &out) -> bool {
    if (it_ == docs_.end()) return false;

    if ((*it_).error()) {
      spdlog::error("Failed to parse document: {}", simdjson::error_message((*it_).error()));
      ++it_;
      return GetNextRow(out);
    }

    out.clear();

    // TODO: (andi) Profile and optimize
    for (auto field : (*it_)->get_object()) {
      std::string_view key_view;
      auto error = field->unescaped_key().get(key_view);
      if (error) continue;

      TypedValue::TString key{key_view, resource_};
      auto maybe_val = field->value();
      auto val = ToTypedValue(maybe_val, resource_);
      out.emplace(std::move(key), std::move(val));
    }

    ++it_;
    return true;
  }

 private:
  std::string file_;
  std::pmr::memory_resource *resource_;
  simdjson::ondemand::parser parser_;
  simdjson::padded_string content_;
  simdjson::ondemand::document_stream docs_;
  simdjson::ondemand::document_stream::iterator it_;
};

JsonlReader::JsonlReader(std::string file, std::pmr::memory_resource *resource)
    : pimpl_{std::make_unique<JsonlReader::impl>(std::move(file), resource)} {}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

}  // namespace memgraph::query
