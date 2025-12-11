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

/**
 * @file
 *
 * This file contains utilities for parsing CSV files.
 *
 */

module;

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <variant>

#include "utils/exceptions.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

export module memgraph.csv.parsing;

import memgraph.utils.aws;

namespace {
template <typename T>
concept Streamable = requires(T t) {
  { t.GetStream() } -> std::convertible_to<std::istream &>;
};

}  // namespace

export namespace memgraph::csv {

class CsvReadException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(CsvReadException)
};

class FileCsvSource {
 public:
  explicit FileCsvSource(std::filesystem::path path);
  std::istream &GetStream();

 private:
  std::filesystem::path path_;
  std::ifstream stream_;
};

class S3CsvSource {
 public:
  explicit S3CsvSource(std::string uri, utils::S3Config const &s3_config);

  std::istream &GetStream();

 private:
  std::stringstream stream_;
};

class StreamCsvSource {
 public:
  explicit StreamCsvSource(std::stringstream stream);
  std::istream &GetStream();

 private:
  std::stringstream stream_;
};

class UrlCsvSource : public StreamCsvSource {
 public:
  explicit UrlCsvSource(std::string url);
};

static_assert(Streamable<FileCsvSource>);
static_assert(Streamable<S3CsvSource>);
static_assert(Streamable<UrlCsvSource>);
static_assert(Streamable<StreamCsvSource>);

class CsvSource {
 public:
  static auto Create(std::string csv_location, std::optional<utils::S3Config> s3_cfg) -> CsvSource;
  template <Streamable T>
  explicit CsvSource(T source) : source_{std::move(source)} {}

  std::istream &GetStream();

 private:
  std::variant<FileCsvSource, UrlCsvSource, StreamCsvSource, S3CsvSource> source_;
};

class Reader {
 public:
  struct Config {
    Config() = default;
    Config(const bool with_header, const bool ignore_bad, std::optional<utils::pmr::string> delim,
           std::optional<utils::pmr::string> qt)
        : with_header(with_header), ignore_bad(ignore_bad), delimiter(std::move(delim)), quote(std::move(qt)) {
      // delimiter + quote can not be empty
      if (delimiter && delimiter->empty()) delimiter.reset();
      if (quote && quote->empty()) quote.reset();
    }

    bool with_header{false};
    bool ignore_bad{false};
    std::optional<utils::pmr::string> delimiter;
    std::optional<utils::pmr::string> quote;
  };

  using Row = utils::pmr::vector<utils::pmr::string>;
  using Header = utils::pmr::vector<utils::pmr::string>;

  explicit Reader(CsvSource source, Config cfg, utils::MemoryResource *mem = utils::NewDeleteResource());

  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  Reader(Reader &&) = default;
  Reader &operator=(Reader &&) = default;

  ~Reader() = default;

  struct ParseError {
    enum class ErrorCode : uint8_t { BAD_HEADER, NO_CLOSING_QUOTE, UNEXPECTED_TOKEN, BAD_NUM_OF_COLUMNS, NULL_BYTE };
    ParseError(ErrorCode code, std::string message) : code(code), message(std::move(message)) {}

    ErrorCode code;
    std::string message;
  };

  using ParsingResult = std::expected<Row, ParseError>;

  bool HasHeader() const;
  auto GetHeader() const -> Header const &;
  auto GetNextRow(utils::MemoryResource *mem) -> std::optional<Row>;

  void Reset();

 private:
  // Some implementation issues that need clearing up, but this is mainly because
  // I don't want `boost/iostreams/filtering_stream.hpp` included in this header file
  // Because it causes issues when combined with antlr headers
  // When we have C++20 modules this can be fixed
  struct impl;
  std::unique_ptr<impl, void (*)(impl *)> pimpl;
};

}  // namespace memgraph::csv
