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
#include <utility>

#include "ctre.hpp"
#include "flags/run_time_configurable.hpp"
#include "requests/requests.hpp"
#include "simdjson.h"
#include "spdlog/spdlog.h"

#include "query/typed_value.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/likely.hpp"

module memgraph.query.jsonl.reader;

namespace {
using memgraph::query::TypedValue;
using simdjson::ondemand::json_type;
using simdjson::ondemand::number_type;

void IterateObject(simdjson::ondemand::object &obj, auto &out, memgraph::utils::MemoryResource *resource);

// .value() method may fail and in that the exception will be thrown.
auto ToTypedValue(simdjson::ondemand::value &val, memgraph::utils::MemoryResource *resource) -> TypedValue {
  switch (val.type()) {
    case json_type::null: {
      return TypedValue{resource};
    }
    case json_type::boolean: {
      return TypedValue{val.get_bool().value(), resource};
    }
    case json_type::number: {
      auto const num_type = val.get_number_type().value();
      switch (num_type) {
        case number_type::floating_point_number: {
          return TypedValue{val.get_double().value(), resource};
        }
        case number_type::signed_integer: {
          return TypedValue{val.get_int64().value(), resource};
        }
        case number_type::unsigned_integer: {
          // NOTE: uint64_t read as int64_t
          return TypedValue{static_cast<int64_t>(val.get_uint64().value()), resource};
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
      TypedValue::TVector t_vec{resource};
      auto arr = val.get_array().value();

      for (auto &&it : arr) {
        t_vec.emplace_back(ToTypedValue(it.value(), resource));
      }

      return TypedValue{std::move(t_vec), resource};
    }
    case json_type::object: {
      TypedValue::TMap t_map{resource};
      auto obj = val.get_object().value();

      IterateObject(obj, t_map, resource);

      return TypedValue{std::move(t_map), resource};
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

void IterateObject(simdjson::ondemand::object &obj, auto &out, memgraph::utils::MemoryResource *resource) {
  for (auto &&field : obj) {
    std::string_view key_view;
    // Check for error
    if (UNLIKELY(field.unescaped_key().get(key_view))) continue;
    // NOLINTNEXTLINE
    TypedValue::TString key{key_view, resource};

    auto val = field.value().value();
    auto typed_val = ToTypedValue(val, resource);
    out.emplace(std::move(key), std::move(typed_val));
  }
}

}  // namespace

namespace memgraph::query {

struct JsonlReader::impl {
 public:
  impl(std::string uri, std::pmr::memory_resource *resource, std::function<void()> abort_check)
      : uri_{std::move(uri)}, resource_{resource} {
    InitSimdjsonContent(std::move(abort_check));

    if (UNLIKELY(parser_.iterate_many(content_).get(docs_))) {
      throw utils::BasicException("Failed to create iterator over documents for file {}", uri_);
    }

    it_ = docs_.begin();
  }

  impl(impl const &) = delete;
  impl &operator=(impl const &) = delete;
  impl(impl &&) = delete;
  impl &operator=(impl &&) = delete;

  ~impl() {
    // Delete the file we were using for download
    if (!local_file_.empty()) {
      utils::DeleteFile(local_file_);
    }
  }

  // Performs file download if necessary before loading the file from disk

  void InitSimdjsonContent(std::function<void()> abort_check) {
    constexpr auto url_matcher = ctre::starts_with<"(https?|ftp)://">;
    if (url_matcher(uri_)) {
      auto const base_path = std::filesystem::path{"/tmp"} / std::filesystem::path{uri_}.filename();
      local_file_ = utils::GetUniqueDownloadPath(base_path);
      if (!requests::CreateAndDownloadFile(
              uri_, local_file_, memgraph::flags::run_time::GetFileDownloadConnTimeoutSec(), std::move(abort_check))) {
        throw utils::BasicException("Failed to download file {}", uri_);
      }
      content_ = simdjson::padded_string::load(local_file_).value();
    } else {
      content_ = simdjson::padded_string::load(uri_).value();
    }
  }

  auto GetNextRow(Row &out) -> bool {
    if (UNLIKELY(it_ == docs_.end())) return false;

    out.clear();
    auto obj = (*it_)->get_object().value();
    IterateObject(obj, out, resource_);
    ++it_;

    return true;
  }

 private:
  std::string uri_;
  std::pmr::memory_resource *resource_;

  // Path where the JSONL file was downloaded
  // Had bugs with std::optional
  std::string local_file_;
  simdjson::ondemand::parser parser_;
  simdjson::padded_string content_;
  simdjson::ondemand::document_stream docs_;
  simdjson::ondemand::document_stream::iterator it_;
};

JsonlReader::JsonlReader(std::string file, std::pmr::memory_resource *resource, std::function<void()> abort_check)
    // NOLINTNEXTLINE
    : pimpl_{std::make_unique<JsonlReader::impl>(std::move(file), resource, std::move(abort_check))} {}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

}  // namespace memgraph::query
