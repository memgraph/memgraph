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

std::optional<utils::pmr::string> Reader::GetNextLine() {
  utils::pmr::string line(memory_);
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
  MG_ASSERT(line_count_ == 1, fmt::format("Invalid use of {}", __func__));
  return ParseRow();
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
  header_ = *header;
}

[[nodiscard]] bool Reader::HasHeader() const { return read_config_.with_header; }

const std::optional<Reader::Header> &Reader::GetHeader() const { return header_; }

namespace {
enum class CsvParserState : uint8_t {
  INITIAL_FIELD,
  NEXT_FIELD,
  QUOTING,
  NOT_QUOTING,
  EXPECT_DELIMITER,
};

bool SubstringStartsWith(const std::string_view str, size_t pos, const std::string_view what) {
  return utils::StartsWith(utils::Substr(str, pos), what);
}
} // namespace

Reader::ParsingResult Reader::ParseRow() {
  utils::pmr::vector<utils::pmr::string> row(memory_);
  utils::pmr::string column(memory_);

  auto state = CsvParserState::INITIAL_FIELD;

  do {
    const auto maybe_line = GetNextLine();
    if (!maybe_line) {
      // The whole file was processed.
      break;
    }

    for (size_t i = 0; i < maybe_line->size(); ++i) {
      const auto c = (*maybe_line)[i];

      // Line feeds and carriage returns are ignored in CSVs.
      if (c == '\n' || c == '\r') continue;
      // Null bytes aren't allowed in CSVs.
      if (c == '\0') {
        return ParseError(ParseError::ErrorCode::NULL_BYTE,
                          fmt::format("CSV: Line {:d} contains NULL byte", line_count_ - 1));
      }

      switch (state) {
        case CsvParserState::INITIAL_FIELD:
        case CsvParserState::NEXT_FIELD: {
          if (SubstringStartsWith(*maybe_line, i, *read_config_.quote)) {
            // The current field is a quoted field.
            state = CsvParserState::QUOTING;
            i += read_config_.quote->size() - 1;
          } else if (SubstringStartsWith(*maybe_line, i, *read_config_.delimiter)) {
            // The current field has an empty value.
            row.emplace_back("");
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter->size() - 1;
          } else {
            // The current field is a regular field.
            column.push_back(c);
            state = CsvParserState::NOT_QUOTING;
          }
          break;
        }
        case CsvParserState::QUOTING: {
          auto quote_now = SubstringStartsWith(*maybe_line, i, *read_config_.quote);
          auto quote_next = SubstringStartsWith(*maybe_line, i + read_config_.quote->size(), *read_config_.quote);
          if (quote_now && quote_next) {
            // This is an escaped quote character.
            column += *read_config_.quote;
            i += read_config_.quote->size() * 2 - 1;
          } else if (quote_now && !quote_next) {
            // This is the end of the quoted field.
            row.emplace_back(std::move(column));
            state = CsvParserState::EXPECT_DELIMITER;
            i += read_config_.quote->size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::NOT_QUOTING: {
          if (SubstringStartsWith(*maybe_line, i, *read_config_.delimiter)) {
            row.emplace_back(std::move(column));
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter->size() - 1;
          } else {
            column.push_back(c);
          }
          break;
        }
        case CsvParserState::EXPECT_DELIMITER: {
          if (SubstringStartsWith(*maybe_line, i, *read_config_.delimiter)) {
            state = CsvParserState::NEXT_FIELD;
            i += read_config_.delimiter->size() - 1;
          } else {
            return ParseError(ParseError::ErrorCode::UNEXPECTED_TOKEN,
                              fmt::format("CSV Reader: Expected '{}' after '{}', but got '{}' at line {:d}",
                                          *read_config_.delimiter, *read_config_.quote, c, line_count_ - 1));
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
      return ParseError(ParseError::ErrorCode::NO_CLOSING_QUOTE,
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

  // reached the end of file - return empty row
  if (row.empty()) {
    return row;
  }

  // Has header, but the header has already been read and the number_of_columns_
  // is already set. Otherwise, we would get an error every time we'd try to
  // parse the header.
  // Also, if we don't have a header, the 'number_of_columns_' will be 0, so no
  // need to check the number of columns.
  if (UNLIKELY(number_of_columns_ != 0 && row.size() != number_of_columns_)) {
    return ParseError(ParseError::ErrorCode::BAD_NUM_OF_COLUMNS,
                      // ToDo(the-joksim):
                      //    - 'line_count_ - 1' is the last line of a row (as a
                      //      row may span several lines) ==> should have a row
                      //      counter
                      fmt::format("Expected {:d} columns in row {:d}, but got {:d}", number_of_columns_,
                                  line_count_ - 1, row.size()));
  }

  return row;
}

// Returns Reader::Row if the read row if valid;
// Returns std::nullopt if end of file is reached or an error occurred
// making it unreadable;
// @throws CsvReadException if a bad row is encountered, and the ignore_bad is set
// to 'true' in the Reader::Config.
std::optional<Reader::Row> Reader::GetNextRow() {
  auto row = ParseRow();

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
      row = ParseRow();
    } while (row.HasError());
  }

  if (row->empty()) {
    // reached end of file
    return std::nullopt;
  }
  return *row;
}

}  // namespace csv
