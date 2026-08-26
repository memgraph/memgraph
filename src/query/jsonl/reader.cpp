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

module;

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
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

import memgraph.utils.aws;

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

    auto val = field->value();
    auto typed_val = ToTypedValue(val, resource);
    out.emplace(std::move(key), std::move(typed_val));
  }
}

// Supplies the bytes of a JSONL source in order, a chunk at a time.
class ByteSource {
 public:
  ByteSource() = default;
  ByteSource(ByteSource const &) = delete;
  auto operator=(ByteSource const &) -> ByteSource & = delete;
  ByteSource(ByteSource &&) = delete;
  auto operator=(ByteSource &&) -> ByteSource & = delete;
  virtual ~ByteSource() = default;

  /// Reads up to `size` bytes into `out` and returns how many were read. Zero means exhausted.
  virtual auto Read(char *out, std::size_t size) -> std::size_t = 0;
};

class FileByteSource final : public ByteSource {
 public:
  explicit FileByteSource(std::string const &path) : stream_{path, std::ios::binary} {
    if (!stream_.is_open()) {
      throw memgraph::utils::BasicException("Couldn't open file {}", path);
    }
  }

  auto Read(char *out, std::size_t size) -> std::size_t override {
    stream_.read(out, static_cast<std::streamsize>(size));
    return static_cast<std::size_t>(stream_.gcount());
  }

 private:
  std::ifstream stream_;
};

}  // namespace

namespace memgraph::query {

struct JsonlReader::impl {
 public:
  impl(std::string uri, std::optional<utils::S3Config> s3_cfg, std::pmr::memory_resource *resource,
       std::function<void()> abort_check, std::size_t chunk_size)
      : uri_{std::move(uri)}, resource_{resource}, buffer_{std::max<std::size_t>(chunk_size, 1)} {
    source_ = OpenSource(std::move(s3_cfg), std::move(abort_check));
  }

  impl(impl const &) = delete;
  impl &operator=(impl const &) = delete;
  impl(impl &&) = delete;
  impl &operator=(impl &&) = delete;

  ~impl() = default;

  auto GetNextRow(Row &out) -> bool {
    while (true) {
      if (LIKELY(parsed_ && it_ != docs_.end())) {
        out.clear();
        auto obj = (*it_)->get_object().value();
        IterateObject(obj, out, resource_);
        ++it_;
        return true;
      }
      if (!Refill()) return false;
    }
  }

 private:
  // Downloads first where the source is remote. The download still goes to a file; only the parse is
  // streamed at this point.
  auto OpenSource(std::optional<utils::S3Config> s3_cfg, std::function<void()> abort_check)
      -> std::unique_ptr<ByteSource> {
    constexpr auto url_matcher = ctre::starts_with<"(https?|ftp)://">;
    constexpr auto s3_matcher = ctre::starts_with<"s3://">;

    auto const build_base_path = [&]() -> std::filesystem::path {
      return std::filesystem::path{"/tmp"} / std::filesystem::path{uri_}.filename();
    };

    if (url_matcher(uri_)) {
      auto [new_path, file] = utils::CreateUniqueDownloadFile(build_base_path());

      if (!requests::CreateAndDownloadFile(uri_,
                                           std::move(file),
                                           memgraph::flags::run_time::GetFileDownloadConnTimeoutSec(),
                                           std::move(abort_check))) {
        utils::DeleteFile(new_path);
        throw utils::BasicException("Failed to download file {}", uri_);
      }
      return OpenDownloaded(new_path);
    }

    if (s3_matcher(uri_)) {
      DMG_ASSERT(s3_cfg.has_value(), "S3Config doesn't have a value");
      if (auto const res = s3_cfg->Validate(); res.has_value()) {
        throw utils::BasicException(utils::AwsValidationErrorToStr(*res));
      }
      auto const new_path = utils::CreateUniqueDownloadFile(build_base_path()).first;
      if (auto const res = utils::GetS3Object(uri_, *s3_cfg, new_path.string()); !res.has_value()) {
        utils::DeleteFile(new_path);
        throw utils::BasicException(res.error().message);
      }
      return OpenDownloaded(new_path);
    }

    return std::make_unique<FileByteSource>(uri_);
  }

  // The open descriptor keeps the content readable, so the name is no longer needed and a failure
  // later on cannot leave the download behind.
  static auto OpenDownloaded(std::filesystem::path const &path) -> std::unique_ptr<ByteSource> {
    auto source = std::make_unique<FileByteSource>(path.string());
    utils::DeleteFile(path);
    return source;
  }

  // Carries the trailing partial document to the front of the buffer, tops the buffer up from the
  // source and parses again. Returns false once nothing parseable is left.
  auto Refill() -> bool {
    auto const carry = parsed_ ? std::min(docs_.truncated_bytes(), filled_) : std::size_t{0};

    // Release the stream's reference to the buffer before that buffer is moved or reallocated.
    docs_ = simdjson::ondemand::document_stream{};
    parsed_ = false;

    if (carry > 0 && carry < filled_) {
      std::memmove(buffer_.data(), buffer_.data() + (filled_ - carry), carry);
    }
    filled_ = carry;

    // A document larger than the buffer fills it without completing, leaving nowhere to read the
    // rest of it into. Growing is what lets the parser make progress again.
    if (filled_ == buffer_.size()) {
      simdjson::padded_string grown{std::max<std::size_t>(buffer_.size() * 2, 64)};
      std::memcpy(grown.data(), buffer_.data(), filled_);
      buffer_ = std::move(grown);
    }

    auto read_any = false;
    while (filled_ < buffer_.size()) {
      auto const read = source_->Read(buffer_.data() + filled_, buffer_.size() - filled_);
      if (read == 0) break;
      filled_ += read;
      read_any = true;
    }

    // Either the source is spent, or what remains is a fragment it will never complete. A trailing
    // fragment is dropped rather than reported, which is what parsing the whole file at once did.
    if (filled_ == 0 || (!read_any && carry == filled_)) {
      return false;
    }

    if (UNLIKELY(parser_.iterate_many(buffer_.data(), filled_, filled_).get(docs_))) {
      throw utils::BasicException("Failed to create iterator over documents for file {}", uri_);
    }
    it_ = docs_.begin();
    parsed_ = true;
    return true;
  }

  std::string uri_;
  std::pmr::memory_resource *resource_;
  std::unique_ptr<ByteSource> source_;
  simdjson::ondemand::parser parser_;
  simdjson::padded_string buffer_;
  std::size_t filled_{0};
  bool parsed_{false};
  simdjson::ondemand::document_stream docs_;
  simdjson::ondemand::document_stream::iterator it_;
};

JsonlReader::JsonlReader(std::string file, std::optional<utils::S3Config> s3_cfg, std::pmr::memory_resource *resource,
                         std::function<void()> abort_check, std::size_t chunk_size)
    : pimpl_{// NOLINTNEXTLINE
             std::make_unique<JsonlReader::impl>(std::move(file), std::move(s3_cfg), resource, std::move(abort_check),
                                                 chunk_size)} {}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

}  // namespace memgraph::query
