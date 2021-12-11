// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/csv_parsing.hpp"

#include <string_view>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace csv {

using ParseError = Reader::ParseError;

void Reader::InitializeStream() {
  if (!std::filesystem::exists(path_)) {
    throw CsvReadException("CSV file not found: {}", path_.string());
  }
  csv_stream_.open(path_);
  if (!csv_stream_.good()) {
    throw CsvReadException("CSV file {} couldn't be opened!", path_.string());
  }
}

std::optional<utils::pmr::string> Reader::GetNextLine(utils::MemoryResource *mem) {
  utils::pmr::string line(mem);
  if (!std::getline(csv_stream_, line)) {
    // reached end of file or an I/0 error occurred
    if (!csv_stream_.good()) {
      csv_stream_.close();
    }
    return std::nullopt;
  }
  ++line_count_;
  return line;
}

Reader::ParsingResult Reader::ParseHeader() {
  // header must be the very first line in the file
  MG_ASSERT(line_count_ == 1, "Invalid use of {}", __func__);
  return ParseRow(memory_);
}

void Reader::TryInitializeHeader() {
  if (!HasHeader()) {
    return;
  }

  auto header = ParseHeader();
  if (header.HasError()) {
    throw CsvReadException("CSV reading : {}", header.GetError().message);
  }

  if (header->empty()) {
    throw CsvReadException("CSV file {} empty!", path_);
  }

  number_of_columns_ = header->size();
  header_ = std::move(*header);
}

[[nodiscard]] bool Reader::HasHeader() const { return read_config_.with_header; }

const Reader::Header &Reader::GetHeader() const { return header_; }

namespace {
enum class CsvParserState : uint8_t { INITIAL_FIELD, NEXT_FIELD, QUOTING, EXPECT_DELIMITER, DONE };

}  // namespace

Reader::ParsingResult Reader::ParseRow(utils::MemoryResource *mem) {
  utils::pmr::vector<utils::pmr::string> row(mem);
  if (number_of_columns_ != 0) {
    row.reserve(number_of_columns_);
  }

  utils::pmr::string column(memory_);

  auto state = CsvParserState::INITIAL_FIELD;

  do {
    const auto maybe_line = GetNextLine(mem);
    if (!maybe_line) {
      // The whole file was processed.
      break;
    }

    std::string_view line_string_view = *maybe_line;

    // remove '\r' from the end in case we have dos file format
    if (line_string_view.back() == '\r') {
      line_string_view.remove_suffix(1);
    }

    while (state != CsvParserState::DONE && !line_string_view.empty()) {
      const auto c = line_string_view[0];

      // Line feeds and carriage returns are ignored in CSVs.
      if (c == '\n' || c == '\r') {
        line_string_view.remove_prefix(1);
        continue;
      }
      // Null bytes aren't allowed in CSVs.
      if (c == '\0') {
        return ParseError(ParseError::ErrorCode::NULL_BYTE,
                          fmt::format("CSV: Line {:d} contains NULL byte", line_count_ - 1));
      }

      switch (state) {
        case CsvParserState::INITIAL_FIELD:
        case CsvParserState::NEXT_FIELD: {
          if (utils::StartsWith(line_string_view, *read_config_.quote)) {
            // The current field is a quoted field.
            state = CsvParserState::QUOTING;
            line_string_view.remove_prefix(read_config_.quote->size());
          } else if (utils::StartsWith(line_string_view, *read_config_.delimiter)) {
            // The current field has an empty value.
            row.emplace_back("");
            state = CsvParserState::NEXT_FIELD;
            line_string_view.remove_prefix(read_config_.delimiter->size());
          } else {
            // The current field is a regular field.
            const auto delimiter_idx = line_string_view.find(*read_config_.delimiter);
            row.emplace_back(line_string_view.substr(0, delimiter_idx));
            if (delimiter_idx == std::string_view::npos) {
              state = CsvParserState::DONE;
            } else {
              line_string_view.remove_prefix(delimiter_idx + read_config_.delimiter->size());
              state = CsvParserState::NEXT_FIELD;
            }
          }
          break;
        }
        case CsvParserState::QUOTING: {
          const auto quote_now = utils::StartsWith(line_string_view, *read_config_.quote);
          const auto quote_next =
              utils::StartsWith(line_string_view.substr(read_config_.quote->size()), *read_config_.quote);
          if (quote_now && quote_next) {
            // This is an escaped quote character.
            column += *read_config_.quote;
            line_string_view.remove_prefix(read_config_.quote->size() * 2);
          } else if (quote_now) {
            // This is the end of the quoted field.
            row.emplace_back(std::move(column));
            column.clear();
            state = CsvParserState::EXPECT_DELIMITER;
            line_string_view.remove_prefix(read_config_.quote->size());
          } else {
            column.push_back(c);
            line_string_view.remove_prefix(1);
          }
          break;
        }
        case CsvParserState::EXPECT_DELIMITER: {
          if (utils::StartsWith(line_string_view, *read_config_.delimiter)) {
            state = CsvParserState::NEXT_FIELD;
            line_string_view.remove_prefix(read_config_.delimiter->size());
          } else {
            return ParseError(ParseError::ErrorCode::UNEXPECTED_TOKEN,
                              fmt::format("CSV Reader: Expected '{}' after '{}', but got '{}' at line {:d}",
                                          *read_config_.delimiter, *read_config_.quote, c, line_count_ - 1));
          }
          break;
        }
        case CsvParserState::DONE: {
          LOG_FATAL("Invalid state of the CSV parser!");
        }
      }
    }
  } while (state == CsvParserState::QUOTING);

  switch (state) {
    case CsvParserState::INITIAL_FIELD:
    case CsvParserState::DONE:
    case CsvParserState::EXPECT_DELIMITER:
      break;
    case CsvParserState::NEXT_FIELD:
      row.emplace_back("");
      break;
    case CsvParserState::QUOTING: {
      return ParseError(ParseError::ErrorCode::NO_CLOSING_QUOTE,
                        "There is no more data left to load while inside a quoted string. "
                        "Did you forget to close the quote?");
      break;
    }
  }

  // reached the end of file - return empty row
  if (row.empty()) {
    return row;
  }

  // Has header, but the header has already been read and the number_of_columns_
  // is already set. Otherwise, we would get an error every time we'd try to
  // parse the header.
  // Also, if we don't have a header, the 'number_of_columns_' will be 0, so no
  // need to check the number of columns.
  if (number_of_columns_ != 0 && row.size() != number_of_columns_) [[unlikely]] {
    return ParseError(ParseError::ErrorCode::BAD_NUM_OF_COLUMNS,
                      // ToDo(the-joksim):
                      //    - 'line_count_ - 1' is the last line of a row (as a
                      //      row may span several lines) ==> should have a row
                      //      counter
                      fmt::format("Expected {:d} columns in row {:d}, but got {:d}", number_of_columns_,
                                  line_count_ - 1, row.size()));
  }

  return std::move(row);
}

// Returns Reader::Row if the read row if valid;
// Returns std::nullopt if end of file is reached or an error occurred
// making it unreadable;
// @throws CsvReadException if a bad row is encountered, and the ignore_bad is set
// to 'true' in the Reader::Config.
std::optional<Reader::Row> Reader::GetNextRow(utils::MemoryResource *mem) {
  auto row = ParseRow(mem);

  if (row.HasError()) {
    if (!read_config_.ignore_bad) {
      throw CsvReadException("CSV Reader: Bad row at line {:d}: {}", line_count_ - 1, row.GetError().message);
    }
    // try to parse as many times as necessary to reach a valid row
    do {
      spdlog::debug("CSV Reader: Bad row at line {:d}: {}", line_count_ - 1, row.GetError().message);
      if (!csv_stream_.good()) {
        return std::nullopt;
      }
      row = ParseRow(mem);
    } while (row.HasError());
  }

  if (row->empty()) {
    // reached end of file
    return std::nullopt;
  }
  return std::move(*row);
}

}  // namespace csv
