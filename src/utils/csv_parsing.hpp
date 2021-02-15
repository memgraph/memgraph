/**
 * @file
 *
 * This file contains utilities for parsing CSV files.
 *
 */

#pragma once

#include <cstdint>
#include <fstream>
#include <map>
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
    Config(std::string delimiter, std::string quote, bool with_header, bool skip_bad)
        : delimiter(delimiter), quote(quote), with_header(with_header), skip_bad(skip_bad) {}

    std::string delimiter{","};
    std::string quote{"\""};
    bool with_header{false};
    bool skip_bad{false};
  };

  struct Header {
    Header() = default;
    explicit Header(std::optional<std::vector<std::string>> fs) : fields(std::move(fs)) {}
    std::optional<std::vector<std::string>> fields;
  };

  struct Row {
    Row() = default;
    explicit Row(std::vector<std::string> cols) : columns(std::move(cols)) {}
    std::vector<std::string> columns;
  };

  explicit Reader(const std::string &path, Config cfg = {}) : path_(path), read_config_(cfg) {
    InitializeStream();
    if (read_config_.with_header) {
      ParseHeader();
    }
  }

  Reader(const Reader &) = delete;
  Reader &operator=(const Reader &) = delete;

  Reader(Reader &&) = delete;
  Reader &operator=(Reader &&) = delete;

  ~Reader() { csv_stream_.close(); }

  struct ParseError {
    explicit ParseError(std::string msg) : message(std::move(msg)) {}
    std::string message;
  };

  void InitializeStream();

  using ParsingResult = utils::BasicResult<ParseError, Row>;
  std::optional<Row> GetNextRow();

 private:
  std::string path_;
  std::ifstream csv_stream_;
  Config read_config_;
  uint64_t line_count_{1};

  Header header_;

  std::optional<std::string> GetNextLine(std::ifstream &stream);

  void ParseHeader();

  ParsingResult ParseRow(std::ifstream &stream);
};

}  // namespace csv
