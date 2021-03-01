#include "utils/csv_parsing.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/string.hpp"


class CsvReaderTest : public ::testing::Test {
 protected:
  const std::filesystem::path csv_directory{std::filesystem::temp_directory_path() / "csv_testing"};

  void SetUp() override { Clear(); CreateCsvDir(); }

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
  explicit FileWriter(const std::filesystem::path path) { stream_.open(path); }

  FileWriter(const FileWriter &) = delete;
  FileWriter &operator=(const FileWriter &) = delete;

  FileWriter(FileWriter &&) = delete;
  FileWriter &operator=(FileWriter &&) = delete;

  void Close() { stream_.close(); }

  size_t WriteLine(const std::string_view line) {
    if (!stream_.is_open()) {
      return 0;
    }

    stream_ << line << std::endl;

    // including the newline character
    return line.size() + 1;
  }

 private:
  std::ofstream stream_;
};

std::string CreateRow(const std::vector<std::string> &columns, const std::string_view delim) {
  return utils::Join(columns, delim);
}

}  // namespace

TEST_F(CsvReaderTest, CommaDelimiter) {
  // create a file with a valid and an invalid row;
  // the invalid row has wrong delimiters;
  // expect the parser's output to be a single string for the invalid row;
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  const std::vector<std::string> columns1{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns1, ","));

  const std::vector<std::string> columns2{"D", "E", "F"};
  writer.WriteLine(CreateRow(columns2, ";"));

  writer.Close();

  // note - default delimiter is ","
  auto reader = csv::Reader(filepath);

  auto parsed_row = reader.GetNextRow();
  ASSERT_EQ(parsed_row->columns, columns1);

  EXPECT_THROW(reader.GetNextRow(), csv::CsvReadException);
}

TEST_F(CsvReaderTest, SemicolonDelimiter) {
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  const std::string delimiter = ";";
  const std::vector<std::string> columns1{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns1, delimiter));

  const std::vector<std::string> columns2{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns2, ","));

  writer.Close();

  const csv::Reader::Config cfg(delimiter, "\"", false, false);
  auto reader = csv::Reader(filepath, cfg);

  auto parsed_row = reader.GetNextRow();
  ASSERT_EQ(parsed_row->columns, columns1);

  EXPECT_THROW(reader.GetNextRow(), csv::CsvReadException);
}

TEST_F(CsvReaderTest, SkipBad) {
  // create a file with invalid first two rows (containing a string with a
  // missing closing quote);
  // the last row is valid;
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  const std::string delimiter = ",";

  const std::vector<std::string> columns_bad{"A", "B", "\"C"};
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));

  const std::vector<std::string> columns_good{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns_good, delimiter));

  writer.Close();

  {
    // we set the 'skip_bad' flag in the read configuration to 'true';
    // parser's output should be solely the valid row;
    const bool skip_bad = true;
    const csv::Reader::Config cfg(delimiter, "\"", false, skip_bad);
    auto reader = csv::Reader(filepath, cfg);

    auto parsed_row = reader.GetNextRow();
    ASSERT_EQ(parsed_row->columns, columns_good);
  }

  {
    // we set the 'skip_bad' flag in the read configuration to 'false';
    // an exception must be thrown;
    const bool skip_bad = false;
    const csv::Reader::Config cfg(delimiter, "\"", false, skip_bad);
    auto reader = csv::Reader(filepath, cfg);

    EXPECT_THROW(reader.GetNextRow(), csv::CsvReadException);
  }
}

TEST_F(CsvReaderTest, AllRowsValid) {
  // create a file with all rows valid;
  // parser should return 'std::nullopt'
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  const std::string delimiter = ",";

  const std::vector<std::string> columns{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));
  writer.WriteLine(CreateRow(columns, delimiter));

  writer.Close();

  const bool skip_bad = false;
  const csv::Reader::Config cfg(delimiter, "\"", false, skip_bad);
  auto reader = csv::Reader(filepath, cfg);

  while (auto parsed_row = reader.GetNextRow()) {
    ASSERT_EQ(parsed_row->columns, columns);
  }
}

TEST_F(CsvReaderTest, SkipAllRows) {
  // create a file with all rows invalid (containing a string with a missing closing quote);
  // parser should return 'std::nullopt'
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  const std::string delimiter = ",";

  const std::vector<std::string> columns_bad{"A", "B", "\"C"};
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));
  writer.WriteLine(CreateRow(columns_bad, delimiter));

  writer.Close();

  const bool skip_bad = true;
  const csv::Reader::Config cfg(delimiter, "\"", false, skip_bad);
  auto reader = csv::Reader(filepath, cfg);

  auto parsed_row = reader.GetNextRow();
  ASSERT_EQ(parsed_row, std::nullopt);
}
