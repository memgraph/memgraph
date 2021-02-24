#include "utils/csv_parsing.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/string.hpp"


class CsvReaderTest : public ::testing::Test {
 protected:
  std::filesystem::path csv_directory{std::filesystem::temp_directory_path() / "csv_testing"};

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

    return line.size();
  }

 private:
  std::ofstream stream_;
};

std::string CreateRow(const std::vector<std::string>& columns, std::string_view delim) {
  return utils::Join(columns, delim);
}

}  // namespace

TEST_F(CsvReaderTest, ReadFile) {
  const auto filepath = csv_directory / "bla.csv";
  auto writer = FileWriter(filepath);

  std::vector<std::string> columns1{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns1, ","));
  std::vector<std::string> columns2{"A", "B", "C"};
  writer.WriteLine(CreateRow(columns2, ";"));
  writer.Close();

  // default delimiter is ","
  auto reader = csv::Reader(filepath);

  auto parsed_row = reader.GetNextRow();
  ASSERT_EQ((*parsed_row).columns, columns1);


  parsed_row = reader.GetNextRow();
  ASSERT_NE((*parsed_row).columns, columns2);
}
