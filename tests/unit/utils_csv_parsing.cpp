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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "utils/csv_parsing.hpp"

#include "utils/string.hpp"

class CsvReaderTest : public ::testing::TestWithParam<const char *> {
 protected:
  const std::filesystem::path csv_directory{std::filesystem::temp_directory_path() / "csv_testing"};

  void SetUp() override {
    Clear();
    CreateCsvDir();
  }

  void TearDown() override { Clear(); }

 private:
  void CreateCsvDir() {
    if (!std::filesystem::exists(csv_directory)) {
      std::filesystem::create_directory(csv_directory);
    }
  }
  void Clear() {
    if (!std::filesystem::exists(csv_directory)) return;
    std::filesystem::remove_all(csv_directory);
  }
};

namespace {
class FileWriter {
 public:
  explicit FileWriter(const std::filesystem::path path, std::string newline = "\n") : newline_{std::move(newline)} {
    stream_.open(path);
  }

  FileWriter(const FileWriter &) = delete;
  FileWriter &operator=(const FileWriter &) = delete;

  FileWriter(FileWriter &&) = delete;
  FileWriter &operator=(FileWriter &&) = delete;

  void Close() { stream_.close(); }

  size_t WriteLine(const std::string_view line) {
    if (!stream_.is_open()) {
      return 0;
    }

    stream_ << line << newline_;

    // including the newline character
    return line.size() + 1;
  }

 private:
  std::ofstream stream_;
  std::string newline_;
};

std::string CreateRow(const std::vector<std::string> &columns, const std::string_view delim) {
  return utils::Join(columns, delim);
}

auto ToPmrColumns(const std::vector<std::string> &columns) {
  utils::pmr::vector<utils::pmr::string> pmr_columns(utils::NewDeleteResource());
  for (const auto &col : columns) {
    pmr_columns.emplace_back(col);
  }
  return pmr_columns;
}

}  // namespace

TEST_P(CsvReaderTest, CommaDelimiter) {
  // create a file with a single valid row;
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  const std::vector<std::string> columns{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns, ","));

  writer.Close();

  utils::MemoryResource *mem{utils::NewDeleteResource()};

  bool with_header = false;
  bool ignore_bad = false;
  utils::pmr::string delimiter{",", mem};
  utils::pmr::string quote{"\"", mem};

  csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg, mem);

  auto parsed_row = reader.GetNextRow(mem);
  ASSERT_EQ(*parsed_row, ToPmrColumns(columns));
}

TEST_P(CsvReaderTest, SemicolonDelimiter) {
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{";", mem};
  const utils::pmr::string quote{"\"", mem};

  const std::vector<std::string> columns{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns, delimiter));

  writer.Close();

  const bool with_header = false;
  const bool ignore_bad = false;
  const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg, mem);

  auto parsed_row = reader.GetNextRow(mem);
  ASSERT_EQ(*parsed_row, ToPmrColumns(columns));
}

TEST_P(CsvReaderTest, SkipBad) {
  // create a file with invalid first two rows (containing a string with a
  // missing closing quote);
  // the last row is valid;
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{";", mem};
  const utils::pmr::string quote{"\"", mem};

  const std::vector<std::string> columns_bad{"A", "B", "\"\"C"};
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));

  const std::vector<std::string> columns_good{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns_good, delimiter));

  writer.Close();

  {
    // we set the 'ignore_bad' flag in the read configuration to 'true';
    // parser's output should be solely the valid row;
    const bool with_header = false;
    const bool ignore_bad = true;
    const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
    auto reader = csv::Reader(filepath, cfg, mem);

    auto parsed_row = reader.GetNextRow(mem);
    ASSERT_EQ(*parsed_row, ToPmrColumns(columns_good));
  }

  {
    // we set the 'ignore_bad' flag in the read configuration to 'false';
    // an exception must be thrown;
    const bool with_header = false;
    const bool ignore_bad = false;
    const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
    auto reader = csv::Reader(filepath, cfg, mem);

    EXPECT_THROW(reader.GetNextRow(mem), csv::CsvReadException);
  }
}

TEST_P(CsvReaderTest, AllRowsValid) {
  // create a file with all rows valid;
  // parser should return 'std::nullopt'
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{",", mem};
  const utils::pmr::string quote{"\"", mem};

  std::vector<std::string> columns{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));

  writer.Close();

  const bool with_header = false;
  const bool ignore_bad = false;
  const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg);

  const auto pmr_columns = ToPmrColumns(columns);
  while (auto parsed_row = reader.GetNextRow(mem)) {
    ASSERT_EQ(*parsed_row, pmr_columns);
  }
}

TEST_P(CsvReaderTest, SkipAllRows) {
  // create a file with all rows invalid (containing a string with a missing closing quote);
  // parser should return 'std::nullopt'
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{",", mem};
  const utils::pmr::string quote{"\"", mem};

  const std::vector<std::string> columns_bad{"A", "B", "\"\"C"};
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));

  writer.Close();

  const bool with_header = false;
  const bool ignore_bad = true;
  const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg);

  auto parsed_row = reader.GetNextRow(mem);
  ASSERT_EQ(parsed_row, std::nullopt);
}

TEST_P(CsvReaderTest, WithHeader) {
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{",", mem};
  const utils::pmr::string quote{"\"", mem};

  const std::vector<std::string> header{"A", "B", "C"};
  const std::vector<std::string> columns{"1", "2", "3"};
  writer.WriteLine(CreateRow(header, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));

  writer.Close();

  const bool with_header = true;
  const bool ignore_bad = false;
  const csv::Reader::Config cfg(with_header, ignore_bad, delimiter, quote);
  auto reader = csv::Reader(filepath, cfg);

  const auto pmr_header = ToPmrColumns(header);
  ASSERT_EQ(reader.GetHeader(), pmr_header);

  const auto pmr_columns = ToPmrColumns(columns);
  while (auto parsed_row = reader.GetNextRow(mem)) {
    ASSERT_EQ(*parsed_row, pmr_columns);
  }
}

TEST_P(CsvReaderTest, MultilineQuotedString) {
  // create a file with first row valid and the second row containing a quoted
  // string spanning two lines;
  // parser should return two valid rows
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{",", mem};
  const utils::pmr::string quote{"\"", mem};

  const std::vector<std::string> first_row{"A", "B", "C"};
  const std::vector<std::string> multiline_first{"D", "\"E", "\"\"F"};
  const std::vector<std::string> multiline_second{"G\"", "H"};

  writer.WriteLine(CreateRow(first_row, delimiter));
  writer.WriteLine(CreateRow(multiline_first, delimiter));
  writer.WriteLine(CreateRow(multiline_second, delimiter));

  writer.Close();

  const bool with_header = false;
  const bool ignore_bad = true;
  const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg);

  auto parsed_row = reader.GetNextRow(mem);
  ASSERT_EQ(*parsed_row, ToPmrColumns(first_row));

  const std::vector<std::string> expected_multiline{"D", "E,\"FG", "H"};
  parsed_row = reader.GetNextRow(mem);
  ASSERT_EQ(*parsed_row, ToPmrColumns(expected_multiline));
}

TEST_P(CsvReaderTest, EmptyColumns) {
  // create a file with all rows valid;
  // parser should return 'std::nullopt'
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath, GetParam());

  utils::MemoryResource *mem(utils::NewDeleteResource());

  const utils::pmr::string delimiter{",", mem};
  const utils::pmr::string quote{"\"", mem};

  std::vector<std::vector<std::string>> expected_rows{{"", "B", "C"}, {"A", "", "C"}, {"A", "B", ""}};

  for (const auto &row : expected_rows) {
    writer.WriteLine(CreateRow(row, delimiter));
  }

  writer.Close();

  const bool with_header = false;
  const bool ignore_bad = false;
  const csv::Reader::Config cfg{with_header, ignore_bad, delimiter, quote};
  auto reader = csv::Reader(filepath, cfg);

  for (const auto &expected_row : expected_rows) {
    const auto pmr_expected_row = ToPmrColumns(expected_row);
    const auto parsed_row = reader.GetNextRow(mem);
    ASSERT_TRUE(parsed_row.has_value());
    ASSERT_EQ(*parsed_row, pmr_expected_row);
  }
}

INSTANTIATE_TEST_CASE_P(NewlineParameterizedTest, CsvReaderTest, ::testing::Values("\n", "\r\n"));
