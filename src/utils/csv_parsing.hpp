/**
 * @file
 *
 * This file contains utilities for parsing CSV files.
 *
 */

#pragma once

#include <cstdint>
#include <fstream>
#include <optional>
#include <filesystem>
#include <string>
#include <vector>

#include "utils/exceptions.hpp"
#include "utils/result.hpp"

namespace csv {

class CsvReadException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class Reader {
 public:
  struct Config {
    Config(){};
    Config(std::string delimiter, std::string quote, const bool with_header, const bool skip_bad)
        : delimiter(std::move(delimiter)), quote(std::move(quote)), with_header(with_header), skip_bad(skip_bad) {}

    std::string delimiter{","};
    std::string quote{"\""};
    bool with_header{false};
    bool skip_bad{false};
  };

  struct Row {
    Row() = default;
    explicit Row(std::vector<std::string> cols) : columns(std::move(cols)) {}
    std::vector<std::string> columns;
  };

  explicit Reader(const std::filesystem::path &path, const Config cfg = {}) : path_(path), read_config_(cfg) {
    InitializeStream();
    if (read_config_.with_header) {
      header_ = ParseHeader();
    }
  }

  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  Reader(Reader &&) = delete;
  Reader &operator=(Reader &&) = delete;

  ~Reader() {
    if (csv_stream_.is_open()) csv_stream_.close();
  }

  class ParseError {
   public:
    enum class ErrorCode : uint8_t { BAD_HEADER, NO_CLOSING_QUOTE, UNEXPECTED_TOKEN, BAD_NUM_OF_COLUMNS, NULL_BYTE };
    ParseError(ErrorCode code, std::string message) : code(code), message(std::move(message)) {}

    ErrorCode code;
    std::string message;
  };

  using ParsingResult = utils::BasicResult<ParseError, Row>;
  std::optional<Row> GetNextRow();

 private:
  std::filesystem::path path_;
  std::ifstream csv_stream_;
  Config read_config_;
  uint64_t line_count_{1};

  struct Header {
    Header() = default;
    explicit Header(std::vector<std::string> fs) : fields(std::move(fs)) {}

    Header(const Header &other) = default;
    Header &operator=(const Header &other) = default;

    Header(Header &&other) = default;
    Header &operator=(Header &&other) = default;

    std::vector<std::string> fields;
  };

  std::optional<Header> header_{};

  void InitializeStream();

  std::optional<std::string> GetNextLine();

  std::optional<Header> ParseHeader();

  ParsingResult ParseRow();
};

}  // namespace csv
