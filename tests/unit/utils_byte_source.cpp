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

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>

#include <unistd.h>

#include <gtest/gtest.h>

#include "utils/byte_source.hpp"
#include "utils/exceptions.hpp"

using memgraph::utils::FileByteSource;

namespace {
auto WriteTempFile(std::string_view contents) -> std::filesystem::path {
  auto path = std::filesystem::temp_directory_path() /
              ("byte_source_" + std::to_string(::getpid()) + "_" + std::to_string(std::rand()) + ".bin");
  std::ofstream out{path, std::ios::binary};
  out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  return path;
}

auto DrainToString(memgraph::utils::ByteSource &source) -> std::string {
  std::string seen;
  std::array<char, 7> buffer{};
  while (auto const read = source.Read(buffer.data(), buffer.size())) {
    seen.append(buffer.data(), read);
  }
  return seen;
}
}  // namespace

TEST(FileByteSourceTest, HandsOverEveryByteOfTheFile) {
  auto const path = WriteTempFile("the quick brown fox jumps over the lazy dog");
  FileByteSource source{path.string()};

  EXPECT_EQ(DrainToString(source), "the quick brown fox jumps over the lazy dog");

  std::filesystem::remove(path);
}

TEST(FileByteSourceTest, AnEmptyFileIsExhaustedStraightAway) {
  auto const path = WriteTempFile("");
  FileByteSource source{path.string()};

  std::array<char, 4> buffer{};
  EXPECT_EQ(source.Read(buffer.data(), buffer.size()), 0U);

  std::filesystem::remove(path);
}

TEST(FileByteSourceTest, ReadingPastTheEndKeepsReportingExhausted) {
  auto const path = WriteTempFile("ab");
  FileByteSource source{path.string()};

  std::array<char, 8> buffer{};
  EXPECT_EQ(source.Read(buffer.data(), buffer.size()), 2U);
  EXPECT_EQ(source.Read(buffer.data(), buffer.size()), 0U);
  EXPECT_EQ(source.Read(buffer.data(), buffer.size()), 0U);

  std::filesystem::remove(path);
}

TEST(FileByteSourceTest, AskingForNothingReadsNothingAndLeavesTheRestToCome) {
  auto const path = WriteTempFile("abc");
  FileByteSource source{path.string()};

  EXPECT_EQ(source.Read(nullptr, 0), 0U);
  EXPECT_EQ(DrainToString(source), "abc");

  std::filesystem::remove(path);
}

TEST(FileByteSourceTest, AFileThatCannotBeOpenedIsReportedAtConstruction) {
  EXPECT_THROW(FileByteSource{"/nonexistent/directory/nothing-here.bin"}, memgraph::utils::BasicException);
}

// A read that fails reports no bytes, which on its own is indistinguishable from reaching the end.
// Taken for the end it would stop a load early and call the result complete, so it must be raised.
// A directory is the case that reaches this: it opens, then refuses to be read from.
TEST(FileByteSourceTest, AReadThatFailsIsNotMistakenForTheEndOfTheSource) {
  auto const directory = std::filesystem::temp_directory_path();
  FileByteSource source{directory.string()};

  std::array<char, 8> buffer{};
  EXPECT_THROW(source.Read(buffer.data(), buffer.size()), memgraph::utils::BasicException);
}
