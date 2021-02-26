#include "csv_parsing.hpp"

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

std::optional<std::string> Reader::GetNextLine() {
  std::string line;
  if (!std::getline(csv_stream_, line)) {
    // reached end of file
    if (!csv_stream_.good()) {
      csv_stream_.close();
    }
    return std::nullopt;
  }
  ++line_count_;
  return line;
}

std::optional<Reader::Header> Reader::ParseHeader() {
  // header must be the very first line in the file
  MG_ASSERT(line_count_ == 1, fmt::format("Invalid use of {}", __func__));
  const auto maybe_line = GetNextLine();
  if (!maybe_line) {
    throw CsvReadException("CSV file {} empty!", path_);
  }
  Header header;
  // set the 'number_of_fields_' once this method is implemented fully
  return std::nullopt;
}

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
  std::vector<std::string> row;
  std::string column;

  auto state = CsvParserState::INITIAL_FIELD;

  // must capture line_count_ here because each call of GetNextLine advances it
  auto current_line = line_count_;

  do {
    const auto &maybe_line = GetNextLine();
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
                          fmt::format("CSV: Line {:d} contains NULL byte", line_count_));
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
            return ParseError(ParseError::ErrorCode::UNEXPECTED_TOKEN,
                              fmt::format("CSV Reader: Expected '{}' after '{}', but got '{}'", read_config_.delimiter,
                                          read_config_.quote, c));
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

  // if there's no header, then:
  //    - if we skip bad rows, then the very first __valid__ row will
  //      determine the allowed number of columns
  //    - if we don't skip bad rows, the very first row will determine the allowed
  //      number of columns in all subsequent rows
  if (!read_config_.with_header) {
    if (read_config_.skip_bad) {
      // if number of columns hasn't been set already('number_of_columns_ == 0'), set it
      if (number_of_columns_ == 0) {
        number_of_columns_ = row.size();
      }
    } else {
      if (current_line == 1) {
        number_of_columns_ = row.size();
      }
    }
  }

  if (row.size() != number_of_columns_) {
    return ParseError(ParseError::ErrorCode::BAD_NUM_OF_COLUMNS,
                      fmt::format("CSV Reader: Expected {:d} columns in row {:d}, but got {:d}", number_of_columns_,
                                  current_line, row.size()));
  }

  return Row(row);
}

// Returns Reader::Row if the read row if valid;
// Returns std::nullopt if end of file is reached;
// @throws CsvReadException if a bad row is encountered, and the skip_bad is set
// to 'true' in the Reader::Config.
std::optional<Reader::Row> Reader::GetNextRow() {
  auto row = ParseRow();
  if (row.HasError()) {
    if (!read_config_.skip_bad) {
      throw CsvReadException("Bad row at line {:d}: {}", line_count_, row.GetError().message);
    }
    // try to parse as many times as necessary to reach a valid row
    while (true) {
      row = ParseRow();
      if (!csv_stream_.is_open()) {
        return std::nullopt;
      }
      if (row.HasValue()) {
        break;
      }
    }
  }
  return row.GetValue();
}

}  // namespace csv
