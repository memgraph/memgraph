// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "csv/parsing.hpp"

#include <string_view>

#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <ctre/ctre.hpp>

#include "requests/requests.hpp"
#include "utils/file.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/string.hpp"

using PlainStream = boost::iostreams::filtering_istream;

namespace memgraph::csv {

using ParseError = Reader::ParseError;

struct Reader::impl {
  impl(CsvSource source, Reader::Config cfg, utils::MemoryResource *mem);

  [[nodiscard]] bool HasHeader() const { return read_config_.with_header; }
  [[nodiscard]] auto Header() const -> Header const & { return header_; }

  auto GetNextRow(utils::MemoryResource *mem) -> std::optional<Reader::Row>;

 private:
  void InitializeStream();

  void TryInitializeHeader();

  std::optional<utils::pmr::string> GetNextLine(utils::MemoryResource *mem);

  ParsingResult ParseHeader();

  ParsingResult ParseRow(utils::MemoryResource *mem);

  utils::MemoryResource *memory_;
  std::filesystem::path path_;
  CsvSource source_;
  PlainStream csv_stream_;
  Config read_config_;
  uint64_t line_count_{1};
  uint16_t number_of_columns_{0};
  Reader::Header header_{memory_};
};

Reader::impl::impl(CsvSource source, Reader::Config cfg, utils::MemoryResource *mem)
    : memory_(mem), source_(std::move(source)) {
  read_config_.with_header = cfg.with_header;
  read_config_.ignore_bad = cfg.ignore_bad;
  read_config_.delimiter = cfg.delimiter ? std::move(*cfg.delimiter) : utils::pmr::string{",", memory_};
  read_config_.quote = cfg.quote ? std::move(*cfg.quote) : utils::pmr::string{"\"", memory_};
  InitializeStream();
  TryInitializeHeader();
}

enum class CompressionMethod : uint8_t {
  NONE,
  GZip,
  BZip2,
};

/// Detect compression based on magic sequences
auto DetectCompressionMethod(std::istream &is) -> CompressionMethod {
  // Ensure stream is reset
  auto const on_exit = utils::OnScopeExit{[&]() { is.seekg(std::ios::beg); }};

  // Note we must use bytes for comparison, not char
  //
  std::byte c{};  // this gets reused
  auto const next_byte = [&](std::byte &b) { return bool(is.get(reinterpret_cast<char &>(b))); };
  if (!next_byte(c)) return CompressionMethod::NONE;

  auto const as_bytes = []<typename... Args>(Args... args) {
    return std::array<std::byte, sizeof...(Args)>{std::byte(args)...};
  };

  // Gzip - 0x1F8B
  constexpr static auto gzip_seq = as_bytes(0x1F, 0x8B);
  if (c == gzip_seq[0]) {
    if (!next_byte(c) || c != gzip_seq[1]) return CompressionMethod::NONE;
    return CompressionMethod::GZip;
  }

  // BZip2 - 0x425A68
  constexpr static auto bzip_seq = as_bytes(0x42, 0x5A, 0x68);
  if (c == bzip_seq[0]) {
    if (!next_byte(c) || c != bzip_seq[1]) return CompressionMethod::NONE;
    if (!next_byte(c) || c != bzip_seq[2]) return CompressionMethod::NONE;
    return CompressionMethod::BZip2;
  }
  return CompressionMethod::NONE;
}

Reader::Reader(CsvSource source, Reader::Config cfg, utils::MemoryResource *mem)
    : pimpl{new impl{std::move(source), std::move(cfg), mem}, [](impl *p) { delete p; }} {}

void Reader::impl::InitializeStream() {
  auto &source = source_.GetStream();

  auto const method = DetectCompressionMethod(source);
  switch (method) {
    case CompressionMethod::GZip:
      csv_stream_.push(boost::iostreams::gzip_decompressor{});
      break;
    case CompressionMethod::BZip2:
      csv_stream_.push(boost::iostreams::bzip2_decompressor{});
      break;
    default:
      break;
  }

  csv_stream_.push(source);
  MG_ASSERT(csv_stream_.auto_close(), "Should be 'auto close' for correct operation");
  MG_ASSERT(csv_stream_.is_complete(), "Should be 'complete' for correct operation");
}

std::optional<utils::pmr::string> Reader::impl::GetNextLine(utils::MemoryResource *mem) {
  utils::pmr::string line(mem);
  if (!std::getline(csv_stream_, line)) {
    // reached end of file or an I/0 error occurred
    if (!csv_stream_.good()) {
      csv_stream_.reset();  // this will close the file_stream_ and clear the chain
    }
    return std::nullopt;
  }
  ++line_count_;
  return std::move(line);
}

Reader::ParsingResult Reader::impl::ParseHeader() {
  // header must be the very first line in the file
  MG_ASSERT(line_count_ == 1, "Invalid use of {}", __func__);
  return ParseRow(memory_);
}

void Reader::impl::TryInitializeHeader() {
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

[[nodiscard]] bool Reader::HasHeader() const { return pimpl->HasHeader(); }

const Reader::Header &Reader::GetHeader() const { return pimpl->Header(); }

namespace {
enum class CsvParserState : uint8_t { INITIAL_FIELD, NEXT_FIELD, QUOTING, EXPECT_DELIMITER, DONE };

}  // namespace

Reader::ParsingResult Reader::impl::ParseRow(utils::MemoryResource *mem) {
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
          const auto quote_size = read_config_.quote->size();
          const auto quote_now = utils::StartsWith(line_string_view, *read_config_.quote);
          const auto quote_next = quote_size <= line_string_view.size() &&
                                  utils::StartsWith(line_string_view.substr(quote_size), *read_config_.quote);
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

std::optional<Reader::Row> Reader::impl::GetNextRow(utils::MemoryResource *mem) {
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

// Returns Reader::Row if the read row if valid;
// Returns std::nullopt if end of file is reached or an error occurred
// making it unreadable;
// @throws CsvReadException if a bad row is encountered, and the ignore_bad is set
// to 'true' in the Reader::Config.
std::optional<Reader::Row> Reader::GetNextRow(utils::MemoryResource *mem) { return pimpl->GetNextRow(mem); }

FileCsvSource::FileCsvSource(std::filesystem::path path) : path_(std::move(path)) {
  if (!std::filesystem::exists(path_)) {
    throw CsvReadException("CSV file not found: {}", path_.string());
  }
  stream_.open(path_);
  if (!stream_.good()) {
    throw CsvReadException("CSV file {} couldn't be opened!", path_.string());
  }
}
std::istream &FileCsvSource::GetStream() { return stream_; }

std::istream &CsvSource::GetStream() {
  return *std::visit([](auto &&source) { return std::addressof(source.GetStream()); }, source_);
}

auto CsvSource::Create(const utils::pmr::string &csv_location) -> CsvSource {
  constexpr auto protocol_matcher = ctre::starts_with<"(https?|ftp)://">;
  if (protocol_matcher(csv_location)) {
    return csv::UrlCsvSource{csv_location.c_str()};
  }
  return csv::FileCsvSource{csv_location};
}

// Helper for UrlCsvSource
auto urlToStringStream(const char *url) -> std::stringstream {
  auto ss = std::stringstream{};
  if (!requests::DownloadToStream(url, ss)) {
    throw CsvReadException("CSV was unable to be fetched from {}", url);
  }
  return ss;
};

UrlCsvSource::UrlCsvSource(const char *url) : StreamCsvSource{urlToStringStream(url)} {}
}  // namespace memgraph::csv
