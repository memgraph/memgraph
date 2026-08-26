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
#include <exception>
#include <fstream>
#include <functional>
#include <memory>
#include <streambuf>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "ctre.hpp"
#include "flags/run_time_configurable.hpp"
#include "requests/requests.hpp"
#include "simdjson.h"
#include "spdlog/spdlog.h"

#include "query/typed_value.hpp"
#include "utils/data_queue.hpp"
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

  /// Reads into `out` and returns how many bytes were read, which may be fewer than `size` even
  /// though more is still to come. Blocks only while there is nothing at all to return. Zero means
  /// the source is exhausted.
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

// Bridges a transfer that pushes its bytes to a reader that pulls them. The transfer runs on its own
// thread and blocks once the queue is full, so it cannot outrun the parser.
class QueuedByteSource final : public ByteSource {
 public:
  /// Runs on the transfer's own thread. `push` returns false once the reader has stopped, which is
  /// the signal to abandon the transfer.
  using Push = std::function<bool(char const *, std::size_t)>;
  using Transfer = std::function<void(Push const &)>;

  static constexpr std::size_t kMaxBlockBytes = 256U * 1024U;

  QueuedByteSource(std::size_t queued_blocks, Transfer transfer)
      : queue_{queued_blocks}, thread_{[this, transfer = std::move(transfer)]() { Run(transfer); }} {}

  ~QueuedByteSource() override {
    // Refusing further blocks makes the sink short-change curl, which ends the transfer. Without it a
    // reader that stops early would leave the thread blocked on a queue nobody is draining.
    queue_.finish();
  }

  auto Read(char *out, std::size_t size) -> std::size_t override {
    std::size_t taken = 0;
    while (taken < size) {
      if (offset_ == block_.size()) {
        offset_ = 0;
        block_.clear();
        // Wait only for the first block. Handing back what has arrived keeps the parser producing
        // rows, and so the query's abort check running, instead of stalling until the buffer is
        // full: on a slow transfer that wait is what a cancel ends up queueing behind.
        auto const got = taken == 0 ? queue_.pop(block_) : queue_.try_pop(block_);
        if (!got) break;
      }
      auto const take = std::min(size - taken, block_.size() - offset_);
      std::memcpy(out + taken, block_.data() + offset_, take);
      offset_ += take;
      taken += take;
    }

    // finish() happens-before pop() reporting the queue drained, so a failure recorded before it is
    // visible here without further synchronization.
    if (taken == 0 && error_) {
      std::rethrow_exception(error_);
    }
    return taken;
  }

 private:
  void Run(Transfer const &transfer) {
    try {
      transfer([this](char const *data, std::size_t size) {
        // A source decides how much it hands over at a time, so cap what one write can put on the
        // queue. Without this the memory held is the queue depth times whatever the source chose.
        while (size > 0) {
          auto const take = std::min(size, kMaxBlockBytes);
          if (!queue_.push(std::vector<char>{data, data + take})) return false;
          data += take;
          size -= take;
        }
        return true;
      });
    } catch (...) {
      error_ = std::current_exception();
    }
    queue_.finish();
  }

  memgraph::utils::DataQueue<std::vector<char>> queue_;
  std::vector<char> block_;
  std::size_t offset_{0};
  std::exception_ptr error_;
  // Declared last so it is joined before anything the thread touches is destroyed.
  std::jthread thread_;
};

// Lets the AWS SDK write its response body straight onto the queue. Only the write side is used, so
// there is nothing to put back and nowhere to seek.
class PushStreambuf final : public std::streambuf {
 public:
  explicit PushStreambuf(QueuedByteSource::Push const &push) : push_{push} {}

 protected:
  auto xsputn(char const *s, std::streamsize n) -> std::streamsize override {
    if (n <= 0) return 0;
    return push_(s, static_cast<std::size_t>(n)) ? n : 0;
  }

  auto overflow(int_type ch) -> int_type override {
    if (traits_type::eq_int_type(ch, traits_type::eof())) return traits_type::not_eof(ch);
    auto const byte = traits_type::to_char_type(ch);
    return push_(&byte, 1) ? ch : traits_type::eof();
  }

 private:
  QueuedByteSource::Push const &push_;
};

// Enough to keep the transfer busy while a chunk is being parsed, without buffering a meaningful
// amount of the body.
constexpr std::size_t kQueuedBlocks = 4;

}  // namespace

namespace memgraph::query {

struct JsonlReader::impl {
 public:
  impl(std::string uri, std::optional<utils::S3Config> s3_cfg, std::pmr::memory_resource *resource,
       std::function<void()> abort_check, std::size_t chunk_size)
      : uri_{std::move(uri)},
        resource_{resource},
        chunk_size_{std::max<std::size_t>(chunk_size, 1)},
        buffer_{chunk_size_} {
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

    if (url_matcher(uri_)) {
      return std::make_unique<QueuedByteSource>(
          kQueuedBlocks,
          [uri = uri_, abort_check = std::move(abort_check)](QueuedByteSource::Push const &push) mutable {
            auto const sink = [&push](char const *data, std::size_t size) -> std::size_t {
              return push(data, size) ? size : 0;
            };
            if (auto const downloaded = requests::DownloadToSink(
                    uri, sink, memgraph::flags::run_time::GetFileDownloadConnTimeoutSec(), std::move(abort_check));
                !downloaded) {
              throw utils::BasicException("Failed to download file {}: {}", uri, downloaded.error().message);
            }
          });
    }

    if (s3_matcher(uri_)) {
      DMG_ASSERT(s3_cfg.has_value(), "S3Config doesn't have a value");
      if (auto const res = s3_cfg->Validate(); res.has_value()) {
        throw utils::BasicException(utils::AwsValidationErrorToStr(*res));
      }
      return std::make_unique<QueuedByteSource>(
          kQueuedBlocks, [uri = uri_, config = *s3_cfg](QueuedByteSource::Push const &push) {
            PushStreambuf sink{push};
            if (auto const res = utils::GetS3ObjectStreaming(uri, config, sink); !res.has_value()) {
              throw utils::BasicException(res.error().message);
            }
          });
    }

    return std::make_unique<FileByteSource>(uri_);
  }

  // Reallocates the buffer, keeping the first `keep` bytes.
  void Resize(std::size_t size, std::size_t keep) {
    simdjson::padded_string resized{size};
    std::memcpy(resized.data(), buffer_.data(), keep);
    buffer_ = std::move(resized);
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
      Resize(std::max<std::size_t>(buffer_.size() * 2, 64), carry);
    } else if (buffer_.size() > chunk_size_ && carry * 2 <= chunk_size_) {
      // One oversized document would otherwise leave the buffer grown for the rest of the source.
      // Only give the space back once what is being carried fits well inside the configured size, so
      // a run of large documents does not thrash between the two.
      Resize(chunk_size_, carry);
    }

    // One read, then parse whatever turned up. Waiting for a full buffer would hold back every row
    // in it until the slowest byte arrived.
    auto const read = source_->Read(buffer_.data() + filled_, buffer_.size() - filled_);
    filled_ += read;
    auto const read_any = read > 0;

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
  std::size_t chunk_size_;
  simdjson::ondemand::parser parser_;
  simdjson::padded_string buffer_;
  std::size_t filled_{0};
  bool parsed_{false};
  simdjson::ondemand::document_stream docs_;
  simdjson::ondemand::document_stream::iterator it_;
  // Declared last so a source that runs a thread is shut down before the parse state it feeds.
  std::unique_ptr<ByteSource> source_;
};

JsonlReader::JsonlReader(std::string file, std::optional<utils::S3Config> s3_cfg, std::pmr::memory_resource *resource,
                         std::function<void()> abort_check, std::size_t chunk_size)
    : pimpl_{// NOLINTNEXTLINE
             std::make_unique<JsonlReader::impl>(std::move(file), std::move(s3_cfg), resource, std::move(abort_check),
                                                 chunk_size)} {}

JsonlReader::~JsonlReader() {}

auto JsonlReader::GetNextRow(Row &out) -> bool { return pimpl_->GetNextRow(out); }

}  // namespace memgraph::query
