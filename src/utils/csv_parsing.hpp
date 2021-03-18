/**
 * @file
 *
 * This file contains utilities for parsing CSV files.
 *
 */

#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/result.hpp"

namespace csv {

class CsvReadException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class Reader {
 public:
  struct Config {
    Config() = default;
    Config(const bool with_header, const bool ignore_bad, std::optional<utils::pmr::string> delim,
           std::optional<utils::pmr::string> qt)
        : with_header(with_header), ignore_bad(ignore_bad), delimiter(std::move(delim)), quote(std::move(qt)) {}

    bool with_header{false};
    bool ignore_bad{false};
    std::optional<utils::pmr::string> delimiter{};
    std::optional<utils::pmr::string> quote{};
  };

  using Row = utils::pmr::vector<utils::pmr::string>;
  using Header = utils::pmr::vector<utils::pmr::string>;

  Reader() = default;
  explicit Reader(std::filesystem::path path, Config cfg, utils::MemoryResource *mem = utils::NewDeleteResource())
      : path_(std::move(path)), memory_(mem) {
    read_config_.with_header = cfg.with_header;
    read_config_.ignore_bad = cfg.ignore_bad;
    read_config_.delimiter = cfg.delimiter ? std::move(*cfg.delimiter) : utils::pmr::string{",", memory_};
    read_config_.quote = cfg.quote ? std::move(*cfg.quote) : utils::pmr::string{"\"", memory_};
    InitializeStream();
    TryInitializeHeader();
  }

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

  using ParsingResult = utils::BasicResult<ParseError, Row>;
  [[nodiscard]] bool HasHeader() const;
  const std::optional<Header> &GetHeader() const;
  std::optional<Row> GetNextRow();

 private:
  std::filesystem::path path_;
  std::ifstream csv_stream_;
  Config read_config_;
  uint64_t line_count_{1};
  uint16_t number_of_columns_{0};
  std::optional<Header> header_{};
  utils::MemoryResource *memory_;

  void InitializeStream();

  void TryInitializeHeader();

  std::optional<utils::pmr::string> GetNextLine();

  ParsingResult ParseHeader();

  ParsingResult ParseRow();
};

}  // namespace csv
