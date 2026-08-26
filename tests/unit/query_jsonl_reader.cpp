// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <memory_resource>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "query/typed_value.hpp"

import memgraph.query.jsonl.reader;

namespace fs = std::filesystem;

// Small enough that documents straddle chunk boundaries, and not a power of two so the boundaries do
// not line up with anything in the content.
constexpr std::size_t kTinyChunk = 7;

class JsonlReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() / "jsonl_reader_test";
    fs::create_directories(dir_);
  }

  void TearDown() override {
    if (fs::exists(dir_)) {
      fs::remove_all(dir_);
    }
  }

  auto WriteFile(std::string_view name, std::string_view content) -> std::string {
    auto const path = dir_ / name;
    std::ofstream out{path, std::ios::binary};
    out.write(content.data(), static_cast<std::streamsize>(content.size()));
    out.close();
    return path.string();
  }

  static auto ReadIds(std::string const &path, std::size_t chunk_size) -> std::vector<int64_t> {
    auto *resource = std::pmr::new_delete_resource();
    memgraph::query::JsonlReader reader{path, std::nullopt, resource, {}, chunk_size};

    std::vector<int64_t> ids;
    memgraph::query::Row row{resource};
    while (reader.GetNextRow(row)) {
      auto const field = row.find("id");
      ids.push_back(field == row.end() ? -1 : field->second.ValueInt());
    }
    return ids;
  }

  static auto Documents(int count) -> std::string {
    std::string content;
    for (auto i = 1; i <= count; ++i) {
      content += std::format(R"({{"id": {}}})", i);
      content += '\n';
    }
    return content;
  }

  fs::path dir_;
};

TEST_F(JsonlReaderTest, EveryDocumentIsParsedWhenTheChunkIsSmallerThanTheFile) {
  auto const path = WriteFile("many.jsonl", Documents(10));

  EXPECT_EQ(ReadIds(path, kTinyChunk), (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
}

TEST_F(JsonlReaderTest, ADocumentLargerThanTheChunkIsStillParsed) {
  auto const filler = std::string(500, 'x');
  auto const path = WriteFile("big.jsonl", std::format("{{\"id\": 1, \"pad\": \"{}\"}}\n{{\"id\": 2}}\n", filler));

  EXPECT_EQ(ReadIds(path, kTinyChunk), (std::vector<int64_t>{1, 2}));
}

TEST_F(JsonlReaderTest, DocumentsAlternatingAroundTheChunkSizeAreAllParsed) {
  auto const filler = std::string(400, 'x');
  std::string content;
  std::vector<int64_t> expected;
  for (auto i = 1; i <= 12; ++i) {
    // Every third document forces the buffer to grow; the rest let it give the space back.
    content +=
        (i % 3 == 0) ? std::format(R"({{"id": {}, "pad": "{}"}})", i, filler) : std::format(R"({{"id": {}}})", i);
    content += '\n';
    expected.push_back(i);
  }
  auto const path = WriteFile("alternating.jsonl", content);

  EXPECT_EQ(ReadIds(path, kTinyChunk), expected);
}

TEST_F(JsonlReaderTest, ALastDocumentWithoutATrailingNewlineIsKept) {
  auto const path = WriteFile("no_newline.jsonl",
                              R"({"id": 1})"
                              "\n"
                              R"({"id": 2})");

  EXPECT_EQ(ReadIds(path, kTinyChunk), (std::vector<int64_t>{1, 2}));
}

TEST_F(JsonlReaderTest, BlankLinesBetweenDocumentsAreSkipped) {
  auto const path = WriteFile("blanks.jsonl", "{\"id\": 1}\n\n\n{\"id\": 2}\n");

  EXPECT_EQ(ReadIds(path, kTinyChunk), (std::vector<int64_t>{1, 2}));
}

TEST_F(JsonlReaderTest, ATruncatedTrailingDocumentIsDropped) {
  auto const path = WriteFile("truncated.jsonl", "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3");

  EXPECT_EQ(ReadIds(path, kTinyChunk), (std::vector<int64_t>{1, 2}));
}

TEST_F(JsonlReaderTest, AnEmptyFileYieldsNoRows) {
  auto const path = WriteFile("empty.jsonl", "");

  EXPECT_TRUE(ReadIds(path, kTinyChunk).empty());
}

TEST_F(JsonlReaderTest, TheChunkSizeDoesNotChangeTheResult) {
  auto const path = WriteFile("sizes.jsonl", Documents(40));
  auto const expected = ReadIds(path, 1U << 20U);

  ASSERT_EQ(expected.size(), 40U);
  EXPECT_EQ(ReadIds(path, kTinyChunk), expected);
  EXPECT_EQ(ReadIds(path, 13), expected);
  EXPECT_EQ(ReadIds(path, 64), expected);
  EXPECT_EQ(ReadIds(path, 4096), expected);
}
