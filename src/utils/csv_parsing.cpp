#include "csv_parsing.hpp"

#include <spdlog/spdlog.h>
#include <filesystem>
#include <string_view>

#include "utils/file.hpp"
#include "utils/string.hpp"

namespace csv {

void Reader::InitializeStream() {
  if (!std::filesystem::exists(std::filesystem::path(path_))) {
    throw CsvReadException("CSV file not found: {}", path_);
  }
  csv_stream_.open(path_);
}

std::optional<std::string> Reader::GetNextLine(std::ifstream &stream) {
  std::string line;
  if (!std::getline(stream, line)) {
    // reached end of file
    return std::nullopt;
  }
  ++line_count_;
  return line;
}

void Reader::ParseHeader() {
  if (line_count_ > 1) {
    return;
  }  // header must be the very first line in the file
  auto maybe_line = GetNextLine(csv_stream_);
  if (!maybe_line) {
    throw CsvReadException("CSV file {} empty!", path_);
  }
  header_.fields = std::nullopt;
}

enum class CsvParserState {
  INITIAL_FIELD,
  NEXT_FIELD,
  QUOTING,
  NOT_QUOTING,
  EXPECT_DELIMITER,
};

bool SubstringStartsWith(const std::string_view &str, size_t pos, const std::string_view &what) {
  return utils::StartsWith(utils::Substr(str, pos), what);
}

Reader::ParsingResult Reader::ParseRow(std::ifstream &stream) {
  std::vector<std::string> row;
  std::string column;

  auto state = CsvParserState::INITIAL_FIELD;

  do {
    auto maybe_line = GetNextLine(stream);
    if (!maybe_line) {
      // The whole file was processed.
      break;
    }

    for (size_t i = 0; i < (*maybe_line).size(); ++i) {
      auto c = (*maybe_line)[i];

      // Line feeds and carriage returns are ignored in CSVs.
      if (c == '\n' || c == '\r') continue;
      // Null bytes aren't allowed in CSVs.
      if (c == '\0') {
        return ParseError(fmt::format("Line {0:d} contains NULL byte", line_count_));
      }

      switch (state) {
        case CsvParserState::INITIAL_FIELD:
        case CsvParserState::NEXT_FIELD: {
          if (SubstringStartsWith(*maybe_line, i, read_config_.quote)) {
            // The current field is a quoted field.
            state = CsvParserState::QUOTING;
            i += read_config_.quote.size() - 1;
          } else if (SubstringStartsWith(*maybe_line, i, read_config_.delimiter)) {
            // The current field has an empty value.
            row.emplace_back("");
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter.size() - 1;
          } else {
            // The current field is a regular field.
            column.push_back(c);
            state = CsvParserState::NOT_QUOTING;
          }
          break;
        }
        case CsvParserState::QUOTING: {
          auto quote_now = SubstringStartsWith(*maybe_line, i, read_config_.quote);
          auto quote_next = SubstringStartsWith(*maybe_line, i + read_config_.quote.size(), read_config_.quote);
          if (quote_now && quote_next) {
            // This is an escaped quote character.
            column += read_config_.quote;
            i += read_config_.quote.size() * 2 - 1;
          } else if (quote_now && !quote_next) {
            // This is the end of the quoted field.
            row.emplace_back(std::move(column));
            state = CsvParserState::EXPECT_DELIMITER;
            i += read_config_.quote.size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::NOT_QUOTING: {
          if (SubstringStartsWith(*maybe_line, i, read_config_.delimiter)) {
            row.emplace_back(std::move(column));
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter.size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::EXPECT_DELIMITER: {
          if (SubstringStartsWith(*maybe_line, i, read_config_.delimiter)) {
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter.size() - 1;
          } else {
            ParseError(
                fmt::format("Expected '{}' after '{}', but got '{}'", read_config_.delimiter, read_config_.quote, c));
          }
          break;
        }
      }
    }
  } while (state == CsvParserState::QUOTING);

  switch (state) {
    case CsvParserState::INITIAL_FIELD: {
      break;
    }
    case CsvParserState::NEXT_FIELD: {
      row.emplace_back(std::move(column));
      break;
    }
    case CsvParserState::QUOTING: {
      ParseError(
          "There is no more data left to load while inside a quoted string. "
          "Did you forget to close the quote?");
      break;
    }
    case CsvParserState::NOT_QUOTING: {
      row.emplace_back(std::move(column));
      break;
    }
    case CsvParserState::EXPECT_DELIMITER: {
      break;
    }
  }

  return Reader::ParsingResult(Row(row));
}

// Returns Reader::Row if the read row if valid;
// Returns std::nullopt if end of file is reached;
// @throws CsvReadException if a bad row is encountered, and the skip_bad is set
// to 'true' in the Reader::Config.
std::optional<Reader::Row> Reader::GetNextRow() {
  auto row = ParseRow(csv_stream_);
  if (row.HasError()) {
    if (!read_config_.skip_bad) {
      throw CsvReadException("Bad row at line {0:d}: {}", line_count_, row.GetError().message);
    } else {
      // try to parse as many times as necessary to reach a valid row
      while (true) {
        row = ParseRow(csv_stream_);
        if (row.HasValue()) {
          break;
        }
      }
    }
  }
  return row.GetValue();
}  // namespace csv
