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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <array>
#include <csignal>
#include <filesystem>
#include <functional>
#include <mutex>
#include <string_view>
#include <thread>

#include "flags/experimental.hpp"
#include "query/exceptions.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/ddl_abort_helpers.hpp"

#include "storage/v2/exceptions.hpp"
// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())

static constexpr std::string_view test_index = "test_index";
static constexpr std::string_view test_label = "test_label";
static constexpr memgraph::storage::TextSearchConfig default_config{.limit = 10};

class TextIndexTest : public testing::Test {
 public:
  static constexpr std::string_view testSuite = "text_search";
  std::unique_ptr<Storage> storage;

  void SetUp() override { storage = std::make_unique<InMemoryStorage>(); }

  void TearDown() override {
    CleanupTextIndices();
    storage.reset();
  }

  void CreateIndex() const {
    auto unique_acc = this->storage->UniqueAccess();
    const auto label = unique_acc->NameToLabel(test_label.data());

    EXPECT_FALSE(!unique_acc->CreateTextIndex(TextIndexSpec{test_index.data(), label, {}}).has_value());
    ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  static VertexAccessor CreateVertex(Storage::Accessor *accessor, std::string_view title, std::string_view content) {
    VertexAccessor vertex = accessor->CreateVertex();
    MG_ASSERT(vertex.AddLabel(accessor->NameToLabel(test_label)).has_value());
    MG_ASSERT(vertex.SetProperty(accessor->NameToProperty("title"), PropertyValue(title)).has_value());
    MG_ASSERT(vertex.SetProperty(accessor->NameToProperty("content"), PropertyValue(content)).has_value());

    return vertex;
  }

 private:
  void CleanupTextIndices() const {
    // Tantivy performs file merging as a background process, which can lead to file deletion errors when trying to
    // delete the index. To avoid flakiness, we won't fail the test if the index cannot be
    // deleted. Correct approach would be to wait for the merging threads to finish on the mgcxx side.
    constexpr auto max_retries = 5;
    constexpr auto retry_delay = std::chrono::milliseconds(100);
    for (int i = 0; i < max_retries; ++i) {
      auto unique_acc = this->storage->UniqueAccess();
      auto status = unique_acc->DropTextIndex(test_index.data());
      if (status.has_value()) {
        // The drop is deferred to commit time — we must commit to actually
        // flip deferred_drop and tear down the on-disk tantivy directory.
        ASSERT_NO_ERROR(unique_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
        return;
      }
      std::this_thread::sleep_for(retry_delay);
    }
    spdlog::error("Failed to clear text index after {} retries.", max_retries);
  }
};

TEST_F(TextIndexTest, SimpleAbortTest) {
  this->CreateIndex();
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    static constexpr auto index_size = 10;

    // Create multiple nodes within a transaction that will be aborted
    for (int i = 0; i < index_size; i++) {
      [[maybe_unused]] const auto vertex = TextIndexTest_SimpleAbortTest_Test::CreateVertex(
          acc.get(), "title" + std::to_string(i), "content " + std::to_string(i));
    }

    // This is enough to check if abort works
    acc->Abort();
    auto result = acc->TextIndexSearch(test_index.data(), "title.*", text_search_mode::REGEX, default_config);
    EXPECT_EQ(result.size(), 0);
  }
}

TEST_F(TextIndexTest, DeletePropertyTest) {
  this->CreateIndex();
  Gid vertex_gid;
  PropertyValue null_value;

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = TextIndexTest_DeletePropertyTest_Test::CreateVertex(acc.get(), "Test Title", "Test content");
    vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify vertex is found before property deletion
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto result = acc->TextIndexSearch(
        test_index.data(), "data.title:Test", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(result.size(), 1);
  }

  // Remove title property and commit
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(vertex_gid, View::OLD).value();
    MG_ASSERT(vertex.SetProperty(acc->NameToProperty("title"), null_value).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Expect the vertex to not be found when searching for title, as the property was removed
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto result = acc->TextIndexSearch(
        test_index.data(), "data.title:Test", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(result.size(), 0);

    // But content should still be searchable
    result = acc->TextIndexSearch(
        test_index.data(), "data.content:Test", text_search_mode::SPECIFIED_PROPERTIES, default_config);
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
        auto acc = this->storage->Access(memgraph::storage::WRITE);
        [[maybe_unused]] const auto vertex = TextIndexTest_ConcurrencyTest_Test::CreateVertex(
            acc.get(), "Title" + std::to_string(i), "Content for document " + std::to_string(i));
        ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
      });
    }
  }

  // Check that all entries ended up in the index by searching
  auto acc = this->storage->Access(memgraph::storage::WRITE);
  auto results = acc->TextIndexSearch(test_index.data(), "title.*", text_search_mode::REGEX, default_config);
  EXPECT_EQ(results.size(), index_size);
}

TEST_F(TextIndexTest, PinnedSearcherSnapshotConsistency) {
  this->CreateIndex();

  // Commit initial data
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    TextIndexTest_PinnedSearcherSnapshotConsistency_Test::CreateVertex(acc.get(), "Alpha", "first document");
    TextIndexTest_PinnedSearcherSnapshotConsistency_Test::CreateVertex(acc.get(), "Beta", "second document");
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Open a search accessor — its searcher snapshot is pinned on first search
  auto search_acc = this->storage->Access(memgraph::storage::WRITE);
  auto first_search = search_acc->TextIndexSearch(
      test_index.data(), "data.content:document", text_search_mode::SPECIFIED_PROPERTIES, default_config);
  ASSERT_EQ(first_search.size(), 2);

  // Meanwhile, another transaction adds more data and commits
  {
    auto writer_acc = this->storage->Access(memgraph::storage::WRITE);
    TextIndexTest_PinnedSearcherSnapshotConsistency_Test::CreateVertex(writer_acc.get(), "Gamma", "third document");
    ASSERT_NO_ERROR(writer_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Second search in the same accessor should see the SAME snapshot (2 results, not 3)
  auto second_search = search_acc->TextIndexSearch(
      test_index.data(), "data.content:document", text_search_mode::SPECIFIED_PROPERTIES, default_config);
  EXPECT_EQ(second_search.size(), 2);

  // A fresh accessor should see the new data
  {
    auto fresh_acc = this->storage->Access(memgraph::storage::WRITE);
    auto fresh_search = fresh_acc->TextIndexSearch(
        test_index.data(), "data.content:document", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(fresh_search.size(), 3);
  }
}

TEST_F(TextIndexTest, ConcurrentDeleteAddAbortTest) {
  this->CreateIndex();
  Gid initial_vertex_gid;

  // Step 1: Commit one node to the index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto vertex =
        TextIndexTest_ConcurrentDeleteAddAbortTest_Test::CreateVertex(acc.get(), "Initial Title", "Initial content");
    initial_vertex_gid = vertex.Gid();
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Verify initial node is in the index
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto result = acc->TextIndexSearch(
        test_index.data(), "data.title:Initial", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(result.size(), 1);
  }

  // Transaction 1: Delete the initial node (but don't commit yet)
  auto delete_acc = this->storage->Access(memgraph::storage::WRITE);
  {
    auto vertex = delete_acc->FindVertex(initial_vertex_gid, View::OLD).value();
    ASSERT_NO_ERROR(delete_acc->DetachDeleteVertex(&vertex));
  }

  // Transaction 2: Add two new nodes and commit immediately
  auto add_acc = this->storage->Access(memgraph::storage::WRITE);
  {
    [[maybe_unused]] auto vertex1 =
        TextIndexTest_ConcurrentDeleteAddAbortTest_Test::CreateVertex(add_acc.get(), "New Title 1", "New content 1");
    [[maybe_unused]] auto vertex2 =
        TextIndexTest_ConcurrentDeleteAddAbortTest_Test::CreateVertex(add_acc.get(), "New Title 2", "New content 2");
    ASSERT_NO_ERROR(add_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  // Step 3: Abort the delete transaction
  delete_acc->Abort();

  // Step 4: Verify final state - original node should still exist, plus the two new nodes
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);

    // Original node should still be there (delete was aborted)
    auto initial_result = acc->TextIndexSearch(
        test_index.data(), "data.title:Initial", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(initial_result.size(), 1);

    // First new node should be there (add was committed)
    auto new1_result = acc->TextIndexSearch(
        test_index.data(), "data.title:\"New Title 1\"", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(new1_result.size(), 1);

    // Second new node should be there (add was committed)
    auto new2_result = acc->TextIndexSearch(
        test_index.data(), "data.title:\"New Title 2\"", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(new2_result.size(), 1);

    // Total should be 3 nodes (1 original + 2 new)
    auto all_results = acc->TextIndexSearch(test_index.data(), "*", text_search_mode::ALL_PROPERTIES, default_config);
    EXPECT_EQ(all_results.size(), 3);
  }
}

TEST_F(TextIndexTest, CreateTextIndexAbortLeavesNoGhostEntry) {
  TextIndexSpec spec{};
  {
    auto acc = this->storage->UniqueAccess();
    spec = TextIndexSpec{test_index.data(), acc->NameToLabel(test_label.data()), {}};
  }
  memgraph::tests::ExpectCreateAbortLeavesNoGhostEntry(
      this, memgraph::tests::UniqueAcc, [&](auto *acc) { return acc->CreateTextIndex(spec); });
}

TEST_F(TextIndexTest, CreateTextEdgeIndexAbortLeavesNoGhostEntry) {
  static constexpr std::string_view edge_index_name = "test_edge_index";
  TextEdgeIndexSpec spec{};
  {
    auto acc = this->storage->UniqueAccess();
    spec = TextEdgeIndexSpec{
        edge_index_name.data(), acc->NameToEdgeType("TEST_EDGE"), std::vector{acc->NameToProperty("text_prop")}};
  }
  memgraph::tests::ExpectCreateAbortLeavesNoGhostEntry(
      this, memgraph::tests::UniqueAcc, [&](auto *acc) { return acc->CreateTextEdgeIndex(spec); });
  // Drop on commit so TearDown's Drop loop only cleans up the vertex text index.
  {
    auto drop_acc = this->storage->UniqueAccess();
    ASSERT_TRUE(drop_acc->DropTextIndex(edge_index_name.data()).has_value());
    ASSERT_NO_ERROR(drop_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TEST_F(TextIndexTest, DropTextEdgeIndexAbortRestoresIndex) {
  static constexpr std::string_view edge_index_name = "test_edge_index";
  static constexpr std::string_view edge_type_name = "TEST_EDGE";
  {
    auto acc = this->storage->UniqueAccess();
    auto const edge_type = acc->NameToEdgeType(edge_type_name.data());
    auto const prop = acc->NameToProperty("text_prop");
    ASSERT_TRUE(
        acc->CreateTextEdgeIndex(TextEdgeIndexSpec{edge_index_name.data(), edge_type, std::vector{prop}}).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropTextIndex(edge_index_name.data()).has_value());
    acc->Abort();
  }
  {
    // Retry DROP must succeed — the aborted drop should have re-installed the
    // entry so it's visible and droppable again.
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropTextIndex(edge_index_name.data()).has_value())
        << "After an aborted DROP, the text edge index must be visible again and droppable.";
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TEST_F(TextIndexTest, DropTextIndexAbortRestoresIndex) {
  this->CreateIndex();
  {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropTextIndex(test_index.data()).has_value());
    acc->Abort();
  }
  {
    // Retry DROP must succeed — the aborted drop should have re-installed
    // the entry so it's visible and droppable again.
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropTextIndex(test_index.data()).has_value())
        << "After an aborted DROP, the text index must be visible again and droppable.";
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

TEST_F(TextIndexTest, FuzzySearchToleratesSingleEdit) {
  this->CreateIndex();
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    this->CreateVertex(acc.get(), "memgraph", "");
    this->CreateVertex(acc.get(), "memgrap", "");
    this->CreateVertex(acc.get(), "coffee", "");
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto exact = acc->TextIndexSearch(
        test_index.data(), "data.title:memgraph", text_search_mode::SPECIFIED_PROPERTIES, default_config);
    EXPECT_EQ(exact.size(), 1);

    constexpr TextSearchConfig fuzzy_config{.limit = 10, .fuzzy_distance = 1};
    auto fuzzy = acc->TextIndexSearch(
        test_index.data(), "data.title:memgraph", text_search_mode::SPECIFIED_PROPERTIES, fuzzy_config);
    EXPECT_EQ(fuzzy.size(), 2);
  }
}

TEST_F(TextIndexTest, FuzzySearchAllProperties) {
  this->CreateIndex();
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    this->CreateVertex(acc.get(), "memgraph", "");
    this->CreateVertex(acc.get(), "coffee", "");
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto exact = acc->TextIndexSearch(test_index.data(), "memgrap", text_search_mode::ALL_PROPERTIES, default_config);
    EXPECT_EQ(exact.size(), 0);

    constexpr TextSearchConfig fuzzy_config{.limit = 10, .fuzzy_distance = 1};
    auto fuzzy = acc->TextIndexSearch(test_index.data(), "memgrap", text_search_mode::ALL_PROPERTIES, fuzzy_config);
    EXPECT_EQ(fuzzy.size(), 1);
  }
}

TEST_F(TextIndexTest, FuzzyDistanceAboveTwoThrows) {
  this->CreateIndex();
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    this->CreateVertex(acc.get(), "memgraph", "");
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    constexpr TextSearchConfig bad_config{.limit = 10, .fuzzy_distance = 3};
    EXPECT_THROW(acc->TextIndexSearch(
                     test_index.data(), "data.title:memgraph", text_search_mode::SPECIFIED_PROPERTIES, bad_config),
                 memgraph::storage::TextSearchException);
  }
}

namespace {

// Makes file-extending writes fail with EFBIG (SIGXFSZ ignored); unlike chmod, this also applies to root.
class WriteFault {
 public:
  WriteFault() {
    old_handler_ = std::signal(SIGXFSZ, SIG_IGN);
    getrlimit(RLIMIT_FSIZE, &old_limit_);
    const rlimit zero{.rlim_cur = 0, .rlim_max = old_limit_.rlim_max};
    setrlimit(RLIMIT_FSIZE, &zero);
    active_ = true;
  }

  void Lift() {
    if (!active_) return;
    setrlimit(RLIMIT_FSIZE, &old_limit_);
    std::signal(SIGXFSZ, old_handler_);
    active_ = false;
  }

  ~WriteFault() { Lift(); }

  WriteFault(const WriteFault &) = delete;
  WriteFault &operator=(const WriteFault &) = delete;

 private:
  rlimit old_limit_{};
  void (*old_handler_)(int){SIG_DFL};
  bool active_{false};
};

// Fires the callback when ApplyTrackedChanges logs its "retrying once" warning.
class OnRetrySink final : public spdlog::sinks::base_sink<std::mutex> {
 public:
  explicit OnRetrySink(std::function<void()> on_retry) : on_retry_(std::move(on_retry)) {}

 protected:
  void sink_it_(const spdlog::details::log_msg &msg) override {
    if (std::string_view(msg.payload.data(), msg.payload.size()).contains("retrying once")) on_retry_();
  }

  void flush_() override {}

 private:
  std::function<void()> on_retry_;
};

class ScopedRetryHook {
 public:
  explicit ScopedRetryHook(std::function<void()> on_retry) : sink_(std::make_shared<OnRetrySink>(std::move(on_retry))) {
    spdlog::default_logger()->sinks().push_back(sink_);
  }

  ~ScopedRetryHook() {
    auto &sinks = spdlog::default_logger()->sinks();
    std::erase(sinks, sink_);
  }

  ScopedRetryHook(const ScopedRetryHook &) = delete;
  ScopedRetryHook &operator=(const ScopedRetryHook &) = delete;

 private:
  std::shared_ptr<OnRetrySink> sink_;
};

constexpr std::string_view edge_index = "test_edge_index";

}  // namespace

class TextIndexFaultTest : public TextIndexTest {
 public:
  void CommitVertex(std::string_view title) const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    CreateVertex(acc.get(), title, "content");
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  size_t CountVertices(std::string_view index = test_index) const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    return acc->TextIndexSearch(std::string{index}, "*", text_search_mode::ALL_PROPERTIES, default_config).size();
  }

  size_t CountTitle(std::string_view title) const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    return acc
        ->TextIndexSearch(test_index.data(),
                          fmt::format("data.title:{}", title),
                          text_search_mode::SPECIFIED_PROPERTIES,
                          default_config)
        .size();
  }

  void CommitEdge(std::string_view title) const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    auto from = acc->CreateVertex();
    auto to = acc->CreateVertex();
    auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("TEST_EDGE"));
    ASSERT_TRUE(edge.has_value());
    ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("text_prop"), PropertyValue(title)).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  size_t CountEdges() const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    return acc->SearchEdgeTextIndex(std::string{edge_index}, "*", text_search_mode::ALL_PROPERTIES, default_config)
        .size();
  }

  size_t CountEdgeTitle(std::string_view title) const {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    return acc
        ->SearchEdgeTextIndex(std::string{edge_index},
                              fmt::format("data.text_prop:{}", title),
                              text_search_mode::SPECIFIED_PROPERTIES,
                              default_config)
        .size();
  }

  void CreateNamedIndex(std::string_view name) const {
    auto acc = this->storage->UniqueAccess();
    const auto label = acc->NameToLabel(test_label.data());
    ASSERT_TRUE(acc->CreateTextIndex(TextIndexSpec{std::string{name}, label, {}}).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  void DropIndexByName(std::string_view name) const {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->DropTextIndex(std::string{name}).has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }

  void CreateEdgeIndex() const {
    auto acc = this->storage->UniqueAccess();
    ASSERT_TRUE(acc->CreateTextEdgeIndex(TextEdgeIndexSpec{edge_index.data(),
                                                           acc->NameToEdgeType("TEST_EDGE"),
                                                           std::vector{acc->NameToProperty("text_prop")}})
                    .has_value());
    ASSERT_NO_ERROR(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
};

TEST_F(TextIndexFaultTest, FailedCommitIsRetriedAndIndexRecovers) {
  this->CreateIndex();
  this->CommitVertex("before");

  {
    WriteFault fault;
    bool retried = false;
    const ScopedRetryHook hook([&] {
      retried = true;
      fault.Lift();
    });
    this->CommitVertex("during");
    EXPECT_TRUE(retried);
  }

  EXPECT_EQ(this->CountVertices(), 2);
  EXPECT_EQ(this->CountTitle("during"), 1);
  this->CommitVertex("after");
  EXPECT_EQ(this->CountVertices(), 3);
}

TEST_F(TextIndexFaultTest, PersistentFailureMarksIndexOutOfSyncUntilRecreated) {
  this->CreateIndex();
  this->CommitVertex("before");

  int retries = 0;
  {
    const ScopedRetryHook hook([&] { ++retries; });
    const WriteFault fault;
    this->CommitVertex("during");
    EXPECT_EQ(retries, 1);
    this->CommitVertex("after");  // a dirty index is skipped, not retried
    EXPECT_EQ(retries, 1);
  }

  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THROW(acc->TextIndexSearch(test_index.data(), "*", text_search_mode::ALL_PROPERTIES, default_config),
                 memgraph::storage::TextSearchException);
    EXPECT_THROW(acc->TextIndexAggregate(test_index.data(), "*", "{}"), memgraph::storage::TextSearchException);
    EXPECT_TRUE(acc->ApproximateVerticesTextCount(test_index).has_value());
  }

  this->DropIndexByName(test_index);
  this->CreateIndex();
  EXPECT_EQ(this->CountVertices(), 3);
}

// Iteration order is unspecified (absl::flat_hash_map), so several healthy indices expose an early exit anywhere.
TEST_F(TextIndexFaultTest, FailingIndexDoesNotSkipTheOthers) {
  constexpr std::string_view broken = "broken_index";
  constexpr std::array<std::string_view, 3> extra_healthy = {"healthy_1", "healthy_2", "healthy_3"};
  this->CreateIndex();
  this->CreateNamedIndex(broken);
  for (const auto name : extra_healthy) this->CreateNamedIndex(name);
  this->CommitVertex("before");

  std::filesystem::remove_all(std::filesystem::path{Config{}.durability.storage_directory} / kTextIndicesDirectory /
                              broken);
  this->CommitVertex("after");

  EXPECT_EQ(this->CountVertices(test_index), 2);
  for (const auto name : extra_healthy) EXPECT_EQ(this->CountVertices(name), 2);
  EXPECT_THROW(this->CountVertices(broken), memgraph::storage::TextSearchException);

  this->DropIndexByName(broken);
  for (const auto name : extra_healthy) this->DropIndexByName(name);
}

TEST_F(TextIndexFaultTest, TextEdgeIndexRetriesThenGoesOutOfSync) {
  this->CreateEdgeIndex();
  this->CommitEdge("before");

  {
    WriteFault fault;
    bool retried = false;
    const ScopedRetryHook hook([&] {
      retried = true;
      fault.Lift();
    });
    this->CommitEdge("during");
    EXPECT_TRUE(retried);
  }
  EXPECT_EQ(this->CountEdges(), 2);
  EXPECT_EQ(this->CountEdgeTitle("during"), 1);
  this->CommitEdge("after");  // index is not dirty: later commits are still applied
  EXPECT_EQ(this->CountEdges(), 3);

  {
    const WriteFault fault;
    this->CommitEdge("lost");
  }
  EXPECT_THROW(this->CountEdges(), memgraph::storage::TextSearchException);
  {
    auto acc = this->storage->Access(memgraph::storage::WRITE);
    EXPECT_THROW(acc->TextEdgeIndexAggregate(std::string{edge_index}, "*", "{}"),
                 memgraph::storage::TextSearchException);
    EXPECT_TRUE(acc->ApproximateEdgesTextCount(edge_index).has_value());
  }

  this->DropIndexByName(edge_index);
}
