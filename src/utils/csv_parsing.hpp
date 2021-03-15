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
#include "utils/result.hpp"

namespace csv {

class CsvReadException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class Reader {
 public:
  struct Config {
    Config(){};
    Config(const bool with_header, const bool ignore_bad, std::optional<std::string> delim,
           std::optional<std::string> qt)
        : with_header(with_header), ignore_bad(ignore_bad) {
      delimiter = (delim) ? std::move(*delim) : ",";
      quote = (qt) ? std::move(*qt) : "\"";
    }

    bool with_header{false};
    bool ignore_bad{false};
    std::string delimiter{","};
    std::string quote{"\""};
  };

  struct Row {
    Row() = default;
    explicit Row(std::vector<std::string> cols) : columns(std::move(cols)) {}
    std::vector<std::string> columns;
  };

  struct Header {
    Header() = default;
    explicit Header(std::vector<std::string> cols) : columns(std::move(cols)) {}
    std::vector<std::string> columns;
  };

  Reader() = default;

  explicit Reader(std::filesystem::path path, Config cfg = {}) : path_(std::move(path)), read_config_(std::move(cfg)) {
    InitializeStream();
    TryInitializeHeader();
  }

  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  Reader(Reader &&) = default;
  Reader &operator=(Reader &&) = default;

  ~Reader() {
    // is this necessary if the dtor of csv_stream_ closes the file anyway?
    if (csv_stream_.is_open()) csv_stream_.close();
  }

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

  void InitializeStream();

  void TryInitializeHeader();

  std::optional<std::string> GetNextLine();

  ParsingResult ParseHeader();

  ParsingResult ParseRow();
};

}  // namespace csv
