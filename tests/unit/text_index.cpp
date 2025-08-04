// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <sys/types.h>
#include <string_view>
#include <thread>

#include "flags/experimental.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_FALSE((result).HasError())

static constexpr std::string_view test_index = "test_index";
static constexpr std::string_view test_label = "test_label";

class TextIndexTest : public testing::Test {
 public:
  static constexpr std::string_view testSuite = "text_search";
  std::unique_ptr<Storage> storage;

  void SetUp() override {
    memgraph::flags::SetExperimental(memgraph::flags::Experiments::TEXT_SEARCH);
    storage = std::make_unique<InMemoryStorage>();
  }

  void TearDown() override {
    CleanupTextIndices();
    storage.reset();
  }

  void CreateIndex() const {
    auto unique_acc = this->storage->UniqueAccess();
    const auto label = unique_acc->NameToLabel(test_label.data());

    EXPECT_FALSE(unique_acc->CreateTextIndex(TextIndexInfo{test_index.data(), label, {}}).HasError());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase());
  }

  static VertexAccessor CreateVertex(Storage::Accessor *accessor, std::string_view title, std::string_view content) {
    VertexAccessor vertex = accessor->CreateVertex();
    MG_ASSERT(!vertex.AddLabel(accessor->NameToLabel(test_label)).HasError());
    MG_ASSERT(!vertex.SetProperty(accessor->NameToProperty("title"), PropertyValue(title)).HasError());
    MG_ASSERT(!vertex.SetProperty(accessor->NameToProperty("content"), PropertyValue(content)).HasError());

    return vertex;
  }

 private:
  void CleanupTextIndices() const {
    // Tantivy performs file merging as a background process, which can lead to file deletion errors when trying to
    // delete the index. To avoid flakiness, we won't fail the test if the index cannot be
    // deleted. Correct approach would be to wait for the merging threads to finish on the mgcxx side.
    constexpr auto max_retries = 5;
    constexpr auto retry_delay = std::chrono::milliseconds(100);
    auto unique_acc = this->storage->UniqueAccess();
    for (int i = 0; i < max_retries; ++i) {
      auto status = unique_acc->DropTextIndex(test_index.data());
      if (!status.HasError()) {
        return;  // Successfully cleared the index
      }
      std::this_thread::sleep_for(retry_delay);
    }
    spdlog::error("Failed to clear text index after {} retries.", max_retries);
  }
};

TEST_F(TextIndexTest, SimpleAbortTest) {
  this->CreateIndex();
  {
    auto acc = this->storage->Access();
    static constexpr auto index_size = 10;

    // Create multiple nodes within a transaction that will be aborted
    for (int i = 0; i < index_size; i++) {
      [[maybe_unused]] const auto vertex = TextIndexTest_SimpleAbortTest_Test::CreateVertex(
          acc.get(), "title" + std::to_string(i), "content " + std::to_string(i));
    }

    // This is enough to check if abort works
    acc->Abort();
    auto result = acc->TextIndexSearch(test_index.data(), "title.*", text_search_mode::REGEX);
    EXPECT_EQ(result.size(), 0);
  }
}

TEST_F(TextIndexTest, DeletePropertyTest) {
  this->CreateIndex();
  Gid vertex_gid;
  PropertyValue null_value;

  {
    auto acc = this->storage->Access();
    auto vertex = TextIndexTest_DeletePropertyTest_Test::CreateVertex(acc.get(), "Test Title", "Test content");
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Verify vertex is found before property deletion
  {
    auto acc = this->storage->Access();
    auto result = acc->TextIndexSearch(test_index.data(), "data.title:Test", text_search_mode::SPECIFIED_PROPERTIES);
    EXPECT_EQ(result.size(), 1);
  }

  // Remove title property and commit
  {
    auto acc = this->storage->Access();
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(!vertex.SetProperty(acc->NameToProperty("title"), null_value).HasError());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
  }

  // Expect the vertex to not be found when searching for title, as the property was removed
  {
    auto acc = this->storage->Access();
    auto result = acc->TextIndexSearch(test_index.data(), "data.title:Test", text_search_mode::SPECIFIED_PROPERTIES);
    EXPECT_EQ(result.size(), 0);

    // But content should still be searchable
    result = acc->TextIndexSearch(test_index.data(), "data.content:Test", text_search_mode::SPECIFIED_PROPERTIES);
    EXPECT_EQ(result.size(), 1);
  }
}

TEST_F(TextIndexTest, ConcurrencyTest) {
  this->CreateIndex();

  const auto index_size = 10;
  {
    std::vector<std::jthread> threads;
    threads.reserve(index_size);
    for (int i = 0; i < index_size; i++) {
      threads.emplace_back([this, i](std::stop_token) {
        auto acc = this->storage->Access();
        [[maybe_unused]] const auto vertex = TextIndexTest_ConcurrencyTest_Test::CreateVertex(
            acc.get(), "Title" + std::to_string(i), "Content for document " + std::to_string(i));
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase());
      });
    }
  }

  // Check that all entries ended up in the index by searching
  auto acc = this->storage->Access();
  auto results = acc->TextIndexSearch(test_index.data(), "title.*", text_search_mode::REGEX);
  EXPECT_EQ(results.size(), index_size);
}
