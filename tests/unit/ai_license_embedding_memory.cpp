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

// Grounds the AI_PLATFORM license memory accounting for vector embeddings.
// Indexing replaces the vertex property with a compact VectorIndexId reference; reads reconstruct
// via operator-new -> graph_memory_tracker (AI limit), NOT the exempt vector_index domain.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <latch>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "memory/db_arena.hpp"
#include "memory/global_memory_control.hpp"
#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/priority_thread_pool.hpp"

using memgraph::storage::PropertyValue;
using memgraph::storage::View;

namespace {

// Small scale for the storage-model / query-semantics tests (fast).
constexpr int kDim = 256;
constexpr int kNumVertices = 1024;
#if USE_JEMALLOC
// graph_memory_tracker moves only when jemalloc commits new arena extents past its dirty-page slack.
// The accounting tests need a working set large enough to force fresh commits after a purge.
constexpr int kDimBig = 512;
constexpr int kNumVerticesBig = 6000;
constexpr int64_t kOneMiB = 1 << 20;
#endif
constexpr auto kNoHandler = nullptr;

memgraph::storage::Config MakeConfig(
    const std::filesystem::path &dir,
    memgraph::storage::IsolationLevel isolation = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
    memgraph::storage::StorageMode mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
  memgraph::storage::Config config{};
  config.transaction.isolation_level = isolation;
  config.salient.storage_mode = mode;
  config.durability.storage_directory = dir;
  config.disk.main_storage_directory = dir / "disk";
  config.gc.type = memgraph::storage::Config::Gc::Type::NONE;
  return config;
}

#if USE_JEMALLOC
int64_t Graph() { return memgraph::utils::graph_memory_tracker.Amount(); }

int64_t VectorIdx() { return memgraph::utils::vector_index_memory_tracker.Amount(); }

int64_t Total() { return memgraph::utils::total_memory_tracker.Amount(); }

// Free unused pages AND decommit jemalloc's retained dirty extents, so graph_memory_tracker starts
// from a lean committed baseline and a fresh working set shows up as new extent commits.
void Stabilize(memgraph::dbms::Database *db) {
  db->storage()->FreeMemory();
  db->storage()->FreeMemory();
  memgraph::memory::PurgeUnusedMemory();
}
#endif

memgraph::storage::VectorIndexSpec MakeSpec(memgraph::storage::LabelId label, memgraph::storage::PropertyId prop,
                                            int dim, int count) {
  return memgraph::storage::VectorIndexSpec{
      .index_name = "emb_index",
      .label_filter =
          memgraph::storage::VectorLabelFilter{.mode = memgraph::storage::VectorMatchMode::SINGLE, .ids = {label}},
      .property = prop,
      .metric_kind = unum::usearch::metric_kind_t::cos_k,
      .dimension = static_cast<std::uint16_t>(dim),
      .resize_coefficient = 2,
      .capacity = static_cast<std::uint64_t>(2 * count),
      .scalar_kind = unum::usearch::scalar_kind_t::f32_k,
  };
}

}  // namespace

class AiLicenseEmbeddingTest : public ::testing::Test {
 protected:
  std::filesystem::path data_dir_{std::filesystem::temp_directory_path() / "mg_test_ai_license_embedding"};

  void SetUp() override { std::filesystem::create_directories(data_dir_); }

  void TearDown() override {
    memgraph::license::global_license_checker.DisableTesting();  // clears any test-applied memory limit
    std::filesystem::remove_all(data_dir_);
  }

  std::unique_ptr<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> MakeDb(const std::string &name) {
    auto dir = data_dir_ / name;
    std::filesystem::create_directories(dir);
    return std::make_unique<memgraph::utils::Gatekeeper<memgraph::dbms::Database>>(MakeConfig(dir));
  }

  static void Populate(memgraph::dbms::Database *db, memgraph::storage::LabelId label,
                       memgraph::storage::PropertyId prop, int count, int dim) {
    memgraph::memory::DbArenaScope scope{&db->Arena()};
    auto acc = db->Access();
    for (int i = 0; i < count; ++i) {
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(label).has_value());
      std::vector<double> embedding(dim, static_cast<double>(i % 7) + 0.5);
      ASSERT_TRUE(v.SetProperty(prop, PropertyValue(embedding)).has_value());
    }
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  static void CreateIndex(memgraph::dbms::Database *db, memgraph::storage::LabelId label,
                          memgraph::storage::PropertyId prop, int dim, int count) {
    memgraph::memory::DbArenaScope scope{&db->Arena()};
    auto acc = db->UniqueAccess();
    ASSERT_TRUE(acc->CreateVectorIndex(MakeSpec(label, prop, dim, count)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // A populated + indexed database with a ready interpreter. Vertex i holds a kDim-long embedding whose
  // elements all equal (i % 7) + 0.5 (see Populate).
  struct QueryEnv {
    QueryEnv(const std::filesystem::path &root, const std::string &name, int count, int dim,
             memgraph::storage::IsolationLevel isolation = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
             memgraph::storage::StorageMode mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL)
        : gk{std::make_unique<memgraph::utils::Gatekeeper<memgraph::dbms::Database>>(
              MakeConfig(MakeDir(root / name), isolation, mode))},
          repl_state{memgraph::storage::ReplicationStateRootPath(MakeConfig(root / name))},
          db{*gk->access()},
          interpreter_context{{},
                              nullptr,
                              nullptr,
                              kNoHandler,
                              &repl_state,
                              system_state,
                              nullptr
#ifdef MG_ENTERPRISE
                              ,
                              nullptr,
                              nullptr
#endif
          },
          faker{&interpreter_context, db},
          label{db->storage()->NameToLabel("Doc")},
          prop{db->storage()->NameToProperty("emb")} {
      Populate(db.get(), label, prop, count, dim);
      CreateIndex(db.get(), label, prop, dim, count);
    }

    static std::filesystem::path MakeDir(const std::filesystem::path &dir) {
      std::filesystem::create_directories(dir);
      return dir;
    }

    std::unique_ptr<memgraph::utils::Gatekeeper<memgraph::dbms::Database>> gk;
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state;
    memgraph::dbms::DatabaseAccess db;
    memgraph::system::System system_state;
    memgraph::query::InterpreterContext interpreter_context;
    InterpreterFaker faker;
    memgraph::storage::LabelId label;
    memgraph::storage::PropertyId prop;
  };

  // The rows of a single-column result, each materialized as a vector of doubles.
  template <class Stream>
  static std::vector<std::vector<double>> EmbeddingRows(const Stream &stream) {
    std::vector<std::vector<double>> rows;
    for (const auto &row : stream.GetResults()) {
      auto &out = rows.emplace_back();
      for (const auto &x : row[0].ValueList()) out.push_back(x.ValueDouble());
    }
    return rows;
  }
};

#if USE_JEMALLOC

// Memory-accounting tests need jemalloc: they assert on graph_memory_tracker / the AI license limit.
class AiLicenseEmbeddingMemoryTest : public AiLicenseEmbeddingTest {
 protected:
  void SetUp() override {
    AiLicenseEmbeddingTest::SetUp();
    // Install the global graph arena hooks so operator-new allocations (reconstructed embeddings,
    // retained result sets) are attributed to graph_memory_tracker, as in a running server.
    memgraph::memory::SetHooks();
  }
};

// Storage model: after indexing, the vertex property is a compact VectorIndexId reference; the
// embedding lives in the vector index. Reading it reconstructs the full vector.
TEST_F(AiLicenseEmbeddingMemoryTest, StorageModel_EmbeddingLivesOnlyInIndex) {
  auto gk = MakeDb("t1");
  auto acc_opt = gk->access();
  ASSERT_TRUE(acc_opt);
  auto &db = *acc_opt;
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");

  Populate(db.get(), label, prop, kNumVertices, kDim);
  Stabilize(db.get());
  const int64_t emb_before = db->DbEmbeddingMemoryUsage();

  CreateIndex(db.get(), label, prop, kDim, kNumVertices);
  Stabilize(db.get());
  const int64_t emb_after = db->DbEmbeddingMemoryUsage();

  EXPECT_GT(emb_after - emb_before, static_cast<int64_t>(kNumVertices) * kDim * 2)
      << "Vector index must now hold the embeddings (db_embedding_memory_tracker -> vector_index domain)";

  // After indexing, the property is a compact VectorIndexId reference, not a raw list; reading it
  // reconstructs the full vector from the index.
  auto racc = db->Access();
  int seen = 0;
  for (auto v : racc->Vertices(View::OLD)) {
    auto pv = v.GetProperty(prop, View::OLD);
    ASSERT_TRUE(pv.has_value());
    ASSERT_TRUE(pv->IsVectorIndexId()) << "Indexed property is stored as a reference, not a raw list";
    EXPECT_EQ(pv->ValueVectorIndexList().size(), static_cast<size_t>(kDim))
        << "Read reconstructs the full vector from the index";
    if (++seen == 4) break;
  }
  EXPECT_EQ(seen, 4);
}

// Materialization accounting: reconstructing embeddings charges graph_memory_tracker (the AI limit),
// leaves the exempt vector_index domain untouched, and does not touch the per-DB query PMR tracker.
TEST_F(AiLicenseEmbeddingMemoryTest, Materialization_ChargesGraphNotVectorIndexNotQuery) {
  auto gk = MakeDb("t2");
  auto acc_opt = gk->access();
  ASSERT_TRUE(acc_opt);
  auto &db = *acc_opt;
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);
  Stabilize(db.get());

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);
  const int64_t graph_before = Graph();
  const int64_t vidx_before = VectorIdx();
  const int64_t dbq_before = db->DbQueryMemoryUsage();

  // Reconstruct and RETAIN every embedding (this is what a materialized result set does).
  std::vector<PropertyValue> held;
  held.reserve(kNumVerticesBig);
  {
    auto racc = db->Access();
    for (auto v : racc->Vertices(View::OLD)) {
      auto pv = v.GetProperty(prop, View::OLD);
      ASSERT_TRUE(pv.has_value());
      held.push_back(std::move(*pv));
    }
  }

  const int64_t graph_delta = Graph() - graph_before;
  const int64_t vidx_delta = VectorIdx() - vidx_before;
  const int64_t dbq_delta = db->DbQueryMemoryUsage() - dbq_before;

  EXPECT_EQ(held.size(), static_cast<size_t>(kNumVerticesBig));
  // graph must grow by a large fraction of the retained embedding bytes (extent-commit granular).
  EXPECT_GT(graph_delta, retained_bytes / 2)
      << "Reconstructed embeddings are charged to graph_memory_tracker (the AI_PLATFORM limit)";
  EXPECT_LT(vidx_delta, retained_bytes / 8) << "The exempt vector_index domain is not re-charged by reading";
  EXPECT_LT(dbq_delta, retained_bytes / 8) << "Reconstruction is not query PMR memory";
  EXPECT_GT(graph_delta, vidx_delta * 4);
  EXPECT_GT(graph_delta, dbq_delta * 4);

  held.clear();
}

// AI limit: with the graph cap below the embedding index size (exempt), count(n.emb) survives
// (reconstruct-and-discard stays bounded), but RETURN n throws OutOfMemory (customer symptom).
TEST_F(AiLicenseEmbeddingMemoryTest, AiGraphLimit_RetainAbortsDiscardSurvives) {
  auto gk = MakeDb("t3");
  auto acc_opt = gk->access();
  ASSERT_TRUE(acc_opt);
  auto &db = *acc_opt;
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);
  Stabilize(db.get());

  const int64_t embeddings_in_index = VectorIdx();
  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);
  // Headroom: generous enough for iteration machinery + a few transiently-reconstructed vectors,
  // but far less than retaining all kNumVerticesBig vectors (retained_bytes).
  const int64_t headroom = 4 * kOneMiB;
  const int64_t limit = Graph() + headroom;
  ASSERT_GT(retained_bytes, 2 * headroom) << "test must retain far more than the headroom";

  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::AI_PLATFORM, limit);

  EXPECT_GT(embeddings_in_index, headroom)
      << "The embedding index is larger than the graph limit, yet coexists with it (it is exempt)";

  // Enable OOM throwing on this thread (the tracker only throws inside an enabler scope).
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;

  // count(n.emb) shape: reconstruct each, inspect, discard. Stays under the cap.
  {
    auto racc = db->Access();
    EXPECT_NO_THROW({
      for (auto v : racc->Vertices(View::OLD)) {
        auto pv = v.GetProperty(prop, View::OLD);
        ASSERT_TRUE(pv.has_value());
        volatile auto sz = pv->ValueVectorIndexList().size();
        (void)sz;  // discard immediately
      }
    });
  }

  // RETURN n shape: reconstruct and retain all. Must blow the graph cap.
  {
    auto racc = db->Access();
    std::vector<PropertyValue> held;
    EXPECT_THROW(
        {
          for (auto v : racc->Vertices(View::OLD)) {
            auto pv = v.GetProperty(prop, View::OLD);
            if (pv.has_value()) held.push_back(std::move(*pv));
          }
        },
        memgraph::utils::OutOfMemoryException);
  }
}

// Query semantics via real interpreter: RETURN n retains all rows in graph memory;
// count(n.emb) folds to one scalar (count == kNumVerticesBig proves every n.emb was evaluated).
TEST_F(AiLicenseEmbeddingMemoryTest, QuerySemantics_ReturnNVsCountEmb) {
  auto gk = MakeDb("t4");
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(MakeConfig(data_dir_ / "t4"))};
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker faker{&interpreter_context, db};

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);

  // RETURN n: the result stream retains every node and its reconstructed embedding.
  Stabilize(db.get());
  int64_t held_return = 0;
  {
    const int64_t before = Graph();
    auto stream = faker.Interpret("MATCH (n) RETURN n");
    held_return = Graph() - before;
    EXPECT_EQ(stream.GetResults().size(), static_cast<size_t>(kNumVerticesBig));
    EXPECT_GT(held_return, retained_bytes / 2)
        << "RETURN n retains the whole materialized result set (embeddings) in graph memory";
  }

  // count(n.emb): reconstructs each embedding transiently, folds to one scalar. Only fixed
  // query-execution machinery persists — far less than RETURN n's retained embeddings.
  Stabilize(db.get());
  int64_t held_count = 0;
  {
    const int64_t before = Graph();
    auto stream = faker.Interpret("MATCH (n) RETURN count(n.emb)");
    held_count = Graph() - before;
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), kNumVerticesBig)
        << "count(n.emb) evaluated (reconstructed) every embedding";
  }

  EXPECT_GT(held_return, held_count * 4)
      << "RETURN n materializes far more embedding memory than count(n.emb): held_return=" << held_return
      << " held_count=" << held_count;
}

// Double-charge: the index copy (exempt, in total) + the materialized copy (graph domain, in total)
// means total_memory_tracker carries the same embedding twice while a result set is live.
TEST_F(AiLicenseEmbeddingMemoryTest, DoubleCharge_IndexCopyPlusMaterializedCopyBothInTotal) {
  auto gk = MakeDb("t5");
  auto acc_opt = gk->access();
  ASSERT_TRUE(acc_opt);
  auto &db = *acc_opt;
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);
  Stabilize(db.get());

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);
  const int64_t vidx_before = VectorIdx();
  const int64_t graph_before = Graph();
  const int64_t total_before = Total();
  const int64_t storage_before = db->DbStorageMemoryUsage();

  // At rest the index already holds ~a full copy of the embeddings, and it is counted in total.
  EXPECT_GT(vidx_before, retained_bytes / 2) << "the exempt vector_index domain holds the embeddings at rest";

  std::vector<PropertyValue> held;
  held.reserve(kNumVerticesBig);
  {
    auto racc = db->Access();
    for (auto v : racc->Vertices(View::OLD)) {
      auto pv = v.GetProperty(prop, View::OLD);
      ASSERT_TRUE(pv.has_value());
      held.push_back(std::move(*pv));
    }
  }

  const int64_t vidx_delta = VectorIdx() - vidx_before;
  const int64_t graph_delta = Graph() - graph_before;
  const int64_t total_delta = Total() - total_before;
  const int64_t storage_delta = db->DbStorageMemoryUsage() - storage_before;

  // The index copy is NOT released by reading (still resident, still counted in total)...
  EXPECT_LT(vidx_delta, retained_bytes / 8) << "index copy is untouched by reading";
  // ...and materialization adds a WHOLE SECOND copy in the graph domain...
  EXPECT_GT(graph_delta, retained_bytes / 2) << "materialized copy lands in the graph domain";
  // ...so total grew by that second copy, on top of the index copy it already carried: double count.
  EXPECT_GT(total_delta, retained_bytes / 2) << "total_memory_tracker now carries the embedding twice";
  // Reads use std::allocator -> default arena, not the DB arena: storage tracker stays flat.
  EXPECT_LT(storage_delta, retained_bytes / 8) << "reconstruction does not touch the per-DB storage arena";

  held.clear();
}

// Lazy sort: GetVectorInto reconstructs into a REUSED scratch buffer (zero per-comparison allocation),
// so the sort's peak graph footprint is O(dim), not O(N*dim); result matches an eager sort.
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1_LazySortBoundedVsEagerSort) {
  auto gk = MakeDb("t6");
  auto acc_opt = gk->access();
  ASSERT_TRUE(acc_opt);
  auto &db = *acc_opt;
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);

  auto acc = db->Access();
  std::vector<memgraph::storage::VertexAccessor> verts;
  verts.reserve(kNumVerticesBig);
  for (auto v : acc->Vertices(View::OLD)) verts.push_back(v);
  ASSERT_EQ(verts.size(), static_cast<size_t>(kNumVerticesBig));

  // Eager reference: reconstruct + RETAIN every embedding, sort them; record the sorted value sequence.
  std::vector<std::vector<float>> eager_sorted;
  {
    std::vector<std::vector<float>> eager;
    eager.reserve(kNumVerticesBig);
    for (auto &va : verts) {
      auto pv = va.GetProperty(prop, View::OLD);
      const auto &lst = pv->ValueVectorIndexList();
      eager.emplace_back(lst.begin(), lst.end());
    }
    std::ranges::sort(eager, [](const auto &a, const auto &b) { return std::ranges::lexicographical_compare(a, b); });
    eager_sorted = std::move(eager);
  }

  // Lazy sort: sort the vertex references; the comparator materializes both operands transiently into
  // two REUSED pre-allocated scratch buffers — zero per-comparison allocation.
  std::vector<float> buf_a(kDimBig);
  std::vector<float> buf_b(kDimBig);
  Stabilize(db.get());
  const int64_t graph_before = Graph();
  std::ranges::sort(verts, [&](const memgraph::storage::VertexAccessor &x, const memgraph::storage::VertexAccessor &y) {
    const bool ok_x = x.GetVectorInto(prop, View::OLD, buf_a);
    const bool ok_y = y.GetVectorInto(prop, View::OLD, buf_b);
    EXPECT_TRUE(ok_x && ok_y);
    return std::ranges::lexicographical_compare(buf_a, buf_b);
  });
  const int64_t lazy_peak = Graph() - graph_before;

  EXPECT_LT(lazy_peak, retained_bytes / 8)
      << "lazy sort must stay O(dim), not O(N*dim): lazy_peak=" << lazy_peak << " retained=" << retained_bytes;

  // Correctness: the lazily-sorted embedding sequence equals the eager one (robust to ties).
  std::vector<std::vector<float>> lazy_sorted;
  lazy_sorted.reserve(verts.size());
  for (auto &va : verts) {
    ASSERT_TRUE(va.GetVectorInto(prop, View::OLD, buf_a));
    lazy_sorted.emplace_back(buf_a.begin(), buf_a.end());
  }
  EXPECT_EQ(lazy_sorted, eager_sorted) << "lazy comparator must produce the same order as an eager sort";
}

// Lazy-value interpreter end-to-end: ORDER BY and DISTINCT stay within the AI graph cap (bounded
// O(dim) transient per step); RETURN n.emb blows the cap (anchors that the cap is genuinely tight).
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1Lazy_OrderByAndDistinctBoundedThroughInterpreter) {
  auto gk = MakeDb("t7");
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(MakeConfig(data_dir_ / "t7"))};
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVerticesBig, kDimBig);
  CreateIndex(db.get(), label, prop, kDimBig, kNumVerticesBig);

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker faker{&interpreter_context, db};

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);
  // Headroom: above the O(N) reference cache (~few MiB of VectorRef/vertex handles) but well below
  // the O(N*dim) cost of materialising every embedding — that gap is the lazy-value win.
  const int64_t headroom = 8 * kOneMiB;
  Stabilize(db.get());
  const int64_t limit = Graph() + headroom;
  ASSERT_GT(retained_bytes, headroom + 2 * kOneMiB)
      << "materialising every embedding must exceed the cap, so (c) is a real abort";

  // AI_PLATFORM: only graph memory is capped; the (larger) embedding index is exempt.
  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::AI_PLATFORM, limit);
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;

  // (a) ORDER BY buffers cheap VectorRef sort keys and materialises O(dim) transiently in the
  //     comparator. LIMIT keeps the ordering from being optimised away; count proves every row sorted.
  {
    auto stream = faker.Interpret("MATCH (n) WITH n ORDER BY n.emb LIMIT 1000000 RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), kNumVerticesBig)
        << "every row was sorted by its embedding without exceeding the AI graph cap";
  }

  // (b) DISTINCT dedups via a lazy hash set: VectorRef keys, transient hash/equality. The populated
  //     data has 7 distinct vectors (value == i%7), so a bounded run yields exactly 7 rows.
  // Purge jemalloc's dirty-page slack left by the prior query so each sub-query runs with fresh
  // headroom under the fixed cap (the tracker is extent-commit granular; freed memory lingers as slack).
  memgraph::memory::PurgeUnusedMemory();
  {
    auto stream = faker.Interpret("MATCH (n) RETURN DISTINCT n.emb AS e");
    EXPECT_EQ(stream.GetResults().size(), 7u)
        << "DISTINCT over embeddings deduped via lazy hash/equality without exceeding the AI graph cap";
  }

  // (c) RETURN n.emb retains a materialised embedding per row in the result stream -> blows the cap.
  memgraph::memory::PurgeUnusedMemory();
  EXPECT_THROW(faker.Interpret("MATCH (n) RETURN n.emb"), memgraph::utils::OutOfMemoryException);
}

#endif  // USE_JEMALLOC

// Map-projection round-trip: `n{.*}` wraps the compact VectorIndexId as a lazy VectorRef;
// Bolt serialisation must materialise the full embedding (a broken lazy path returns an empty list).
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_MapProjectionMaterialisesFullEmbedding) {
  auto gk = MakeDb("t8");
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(MakeConfig(data_dir_ / "t8"))};
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVertices, kDim);
  CreateIndex(db.get(), label, prop, kDim, kNumVertices);

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker faker{&interpreter_context, db};

  auto stream = faker.Interpret("MATCH (n) RETURN n{.*} AS m");
  ASSERT_EQ(stream.GetResults().size(), static_cast<size_t>(kNumVertices));
  for (const auto &row : stream.GetResults()) {
    const auto &m = row[0].ValueMap();
    auto it = m.find("emb");
    ASSERT_NE(it, m.end()) << "n{.*} must include the embedding property";
    const auto &emb = it->second.ValueList();
    // A broken lazy path would hand back an empty (un-reconstructed) list; the full vector must survive.
    EXPECT_EQ(emb.size(), static_cast<size_t>(kDim)) << "lazy map embedding must materialise to full dimension";
  }
}

// A lazy VectorRef must behave exactly like the List<Double> it materialises to. Two consequences the
// value-level code must guarantee: (1) equal values hash identically, or a DISTINCT/GROUP BY hash set
// keeps a lazy ref and its materialised twin as separate rows; (2) the write-path conversion produces
// that same list instead of throwing, so SET/CREATE can copy an embedding.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_HashConsistentAndWritePathMaterialises) {
  auto gk = MakeDb("t9");
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVertices, kDim);
  CreateIndex(db.get(), label, prop, kDim, kNumVertices);

  auto acc = db->Access();
  auto vertices = acc->Vertices(View::NEW);
  auto it = vertices.begin();
  ASSERT_NE(it, vertices.end());
  const memgraph::storage::VertexAccessor sva = *it;

  auto *mem = memgraph::utils::NewDeleteResource();
  const memgraph::query::TypedValue vref{memgraph::query::LazyVectorRef{sva, prop, View::NEW}, mem};
  ASSERT_TRUE(vref.IsVectorRef());

  const memgraph::query::TypedValue materialised = vref.MaterializeVectorRef(mem);
  ASSERT_TRUE(materialised.IsList());
  ASSERT_EQ(materialised.ValueList().size(), static_cast<size_t>(kDim));

  // (1) equal => same hash. A float-vs-double hash mismatch would silently keep both under DISTINCT.
  EXPECT_TRUE((vref == materialised).ValueBool());
  EXPECT_EQ(memgraph::query::TypedValue::Hash{}(vref), memgraph::query::TypedValue::Hash{}(materialised));

  // (2) the ref converts to the same double list a literal assignment would store, not a throw.
  memgraph::storage::PropertyValue pv;
  EXPECT_NO_THROW(pv = vref.ToPropertyValue(nullptr));
  // Storage keeps typed double-lists distinct from generic lists; assert on the query-layer list the
  // stored value round-trips to (the shape a reader actually sees).
  const memgraph::query::TypedValue back{pv, nullptr, mem};
  ASSERT_TRUE(back.IsList());
  EXPECT_EQ(back.ValueList().size(), static_cast<size_t>(kDim));
}

// A lazy VectorRef defers the read, so it must re-resolve the property through the MVCC view it was
// created under. SetProperty converts the new value to a VectorIndexId and updates usearch immediately,
// so usearch only ever holds the live value. A ref read under View::OLD and materialized after the same
// transaction overwrites the embedding must therefore get the pre-SET floats from the delta, which
// carries them; reading usearch (the pre-fix behavior) would yield the 42.0 overwrite.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_MaterializationHonoursMvccView) {
  auto gk = MakeDb("t11");
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVertices, kDim);
  CreateIndex(db.get(), label, prop, kDim, kNumVertices);

  auto acc = db->Access();
  auto vertices = acc->Vertices(View::OLD);
  auto it = vertices.begin();
  ASSERT_NE(it, vertices.end());
  memgraph::storage::VertexAccessor sva = *it;

  auto *mem = memgraph::utils::NewDeleteResource();
  const memgraph::query::TypedValue vref_old{memgraph::query::LazyVectorRef{sva, prop, View::OLD}, mem};
  ASSERT_TRUE(vref_old.IsVectorRef());

  // Overwrite the embedding in the same transaction; usearch now holds the 42.0 vector.
  const std::vector<double> replacement(kDim, 42.0);
  ASSERT_TRUE(sva.SetProperty(prop, PropertyValue(replacement)).has_value());

  const memgraph::query::TypedValue materialised = vref_old.MaterializeVectorRef(mem);
  ASSERT_TRUE(materialised.IsList());
  ASSERT_EQ(materialised.ValueList().size(), static_cast<size_t>(kDim))
      << "OLD-view lazy ref must materialize from the delta-carried floats";
  // Vertex 0's committed embedding is 0.5 everywhere (Populate: (i%7)+0.5); the 42.0 overwrite is NEW.
  EXPECT_FLOAT_EQ(static_cast<float>(materialised.ValueList().front().ValueDouble()), 0.5F)
      << "materialized value must be the committed embedding, not the same-transaction overwrite";
}

// Guards the dirty-read fix in GetVectorInto: another transaction's UNCOMMITTED SET already updated
// usearch, so a reader that took the floats from usearch would see 42.0. The reader must get the
// committed floats from the delta chain, for both of its views and after the writer aborts.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_MaterializationIgnoresOtherTransactionsUncommittedSet) {
  QueryEnv env{data_dir_, "t12", kNumVertices, kDim};
  auto &db = env.db;

  auto writer = db->Access();
  auto reader = db->Access();
  auto vertices = writer->Vertices(View::OLD);
  auto it = vertices.begin();
  ASSERT_NE(it, vertices.end());
  const auto gid = (*it).Gid();
  auto wvertex = *it;
  const std::vector<double> uncommitted(kDim, 42.0);
  ASSERT_TRUE(wvertex.SetProperty(env.prop, PropertyValue(uncommitted)).has_value());

  auto rvertex = reader->FindVertex(gid, View::OLD);
  ASSERT_TRUE(rvertex);
  auto *mem = memgraph::utils::NewDeleteResource();
  for (const auto view : {View::OLD, View::NEW}) {
    const memgraph::query::TypedValue ref{memgraph::query::LazyVectorRef{*rvertex, env.prop, view}, mem};
    const auto floats = ref.MaterializeVectorRef(mem);
    ASSERT_EQ(floats.ValueList().size(), static_cast<size_t>(kDim));
    EXPECT_FLOAT_EQ(static_cast<float>(floats.ValueList().front().ValueDouble()), 0.5F)
        << "reader must see the committed embedding, not the other transaction's uncommitted SET";
  }

  writer->Abort();
  const memgraph::query::TypedValue ref{memgraph::query::LazyVectorRef{*rvertex, env.prop, View::OLD}, mem};
  EXPECT_FLOAT_EQ(static_cast<float>(ref.MaterializeVectorRef(mem).ValueList().front().ValueDouble()), 0.5F);
}

// Guards the read-only gate (EvaluationContext::lazy_vector_refs): a write plan that holds an embedding
// across a statement that rewrites or deletes it must still return the ORIGINAL vector, because a lazy
// ref would re-read storage after the write.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_WritePlansStayEager) {
  constexpr int kCount = 3;
  constexpr int kSmallDim = 8;
  const std::vector<std::vector<double>> originals = {
      std::vector<double>(kSmallDim, 0.5), std::vector<double>(kSmallDim, 1.5), std::vector<double>(kSmallDim, 2.5)};

  const std::vector<std::string> queries = {
      "MATCH (n:Doc) WITH n, n.emb AS e REMOVE n.emb RETURN e ORDER BY e",
      "MATCH (n:Doc) WITH n, n.emb AS e SET n.emb = [x IN range(1, 8) | 42.0] RETURN e ORDER BY e",
      "MATCH (n:Doc) WITH n, n.emb AS e DETACH DELETE n RETURN e ORDER BY e",
  };
  int case_no = 0;
  for (const auto &query : queries) {
    QueryEnv env{data_dir_, "t13_" + std::to_string(case_no++), kCount, kSmallDim};
    auto stream = env.faker.Interpret(query);
    EXPECT_EQ(EmbeddingRows(stream), originals) << query;
  }

  QueryEnv env{data_dir_, "t13_create", kCount, kSmallDim};
  auto stream = env.faker.Interpret(
      "CREATE (n:Doc {emb: [x IN range(1, 8) | toFloat(x)]}) WITH n UNWIND n.emb AS x RETURN count(x) AS c");
  ASSERT_EQ(stream.GetResults().size(), 1u);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), kSmallDim)
      << "an embedding written by the same query must be readable by it";
}

// Guards the IsSnapshotStable() gate: lazy refs are only legal under SNAPSHOT_ISOLATION on in-memory
// transactional storage. Elsewhere reads stay eager and must return identical results.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_SnapshotStableGateAndReadsUnderEveryMode) {
  struct Case {
    memgraph::storage::IsolationLevel isolation;
    memgraph::storage::StorageMode mode;
    bool stable;
  };

  using memgraph::storage::IsolationLevel;
  using memgraph::storage::StorageMode;
  const Case cases[] = {
      {IsolationLevel::SNAPSHOT_ISOLATION, StorageMode::IN_MEMORY_TRANSACTIONAL, true},
      {IsolationLevel::READ_COMMITTED, StorageMode::IN_MEMORY_TRANSACTIONAL, false},
      {IsolationLevel::READ_UNCOMMITTED, StorageMode::IN_MEMORY_TRANSACTIONAL, false},
      {IsolationLevel::SNAPSHOT_ISOLATION, StorageMode::IN_MEMORY_ANALYTICAL, false},
  };
  int case_no = 0;
  for (const auto &c : cases) {
    SCOPED_TRACE(case_no);
    QueryEnv env{data_dir_, "t14_" + std::to_string(case_no++), 14, 8, c.isolation, c.mode};
    {
      auto acc = env.db->Access();
      memgraph::query::DbAccessor dba{acc.get()};
      EXPECT_EQ(dba.IsSnapshotStable(), c.stable);
    }
    auto stream = env.faker.Interpret("MATCH (n:Doc) WITH n ORDER BY n.emb RETURN DISTINCT n.emb AS e");
    const auto rows = EmbeddingRows(stream);
    ASSERT_EQ(rows.size(), 7u);
    for (size_t i = 0; i < rows.size(); ++i) {
      EXPECT_EQ(rows[i], std::vector<double>(8, static_cast<double>(i) + 0.5));
    }
  }
}

// Guards Interpreter::ThrowIfLazyVectorReaderOpen: in an explicit transaction, a write must not run while
// an earlier read query that may hold lazy embedding refs still has unconsumed results.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_ExplicitTxnWriteWaitsForOpenLazyReader) {
  QueryEnv env{data_dir_, "t15", 3, 8};
  auto &faker = env.faker;
  faker.interpreter.BeginTransaction();

  auto [reader, reader_qid] = faker.Prepare("MATCH (n:Doc) RETURN n.emb");
  faker.Pull(&reader, 1, reader_qid);
  ASSERT_TRUE(reader.GetSummary().at("has_more").ValueBool());

  auto [writer, writer_qid] = faker.Prepare("CREATE (:Other)");
  EXPECT_THROW(faker.Pull(&writer, {}, writer_qid), memgraph::query::QueryException);

  faker.Pull(&reader, {}, reader_qid);
  EXPECT_EQ(reader.GetResults().size(), 3u);

  EXPECT_NO_THROW(faker.Pull(&writer, {}, writer_qid)) << "the write runs once the lazy reader is consumed";
  faker.Abort();
}

// Guards the relations port: a VectorRef must compare, order, hash and test emptiness exactly like the
// numeric list it materializes to. The embedding is [0.5, 0.5, 0.5, 0.5] (exact in float and double).
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_BehavesLikeItsListInRelations) {
  QueryEnv env{data_dir_, "t16", 1, 4};

  struct Row {
    const char *literal;
    bool equal;
    bool embedding_less;  // e < literal
    int64_t distinct;     // count of distinct values among {e, literal}
  };

  const Row table[] = {
      {"[0.5, 0.5, 0.5, 0.5]", true, false, 1},       // equal list
      {"[0.5, 0.5, 0.5]", false, false, 2},           // prefix: e is longer, so greater
      {"[0.5, 0.5, 0.5, 0.5, 0.5]", false, true, 2},  // longer: e is a prefix, so less
      {"[0.5, 0.5, 0.5, 1.5]", false, true, 2},       // differs in the last element, larger
      {"[0.5, 0.5, 0.5, 0.25]", false, false, 2},     // differs in the last element, smaller
  };
  for (const auto &row : table) {
    SCOPED_TRACE(row.literal);
    const std::string lit = row.literal;
    {
      auto s = env.faker.Interpret("MATCH (n:Doc) WITH n.emb AS e RETURN e = " + lit + " AS eq, e < " + lit +
                                   " AS lt, isEmpty(e) AS emp");
      ASSERT_EQ(s.GetResults().size(), 1u);
      EXPECT_EQ(s.GetResults()[0][0].ValueBool(), row.equal);
      EXPECT_EQ(s.GetResults()[0][1].ValueBool(), row.embedding_less);
      EXPECT_FALSE(s.GetResults()[0][2].ValueBool()) << "isEmpty(VectorRef) must read the materialized list";
    }
    {
      auto s = env.faker.Interpret("MATCH (n:Doc) WITH n.emb AS e UNWIND [e, " + lit +
                                   "] AS x RETURN count(DISTINCT x) AS c");
      EXPECT_EQ(s.GetResults().at(0)[0].ValueInt(), row.distinct) << "hash/equivalence in an aggregation";
    }
    {
      auto s = env.faker.Interpret("MATCH (n:Doc) WITH n.emb AS e UNWIND [e, " + lit +
                                   "] AS x WITH DISTINCT x RETURN count(x) AS c");
      EXPECT_EQ(s.GetResults().at(0)[0].ValueInt(), row.distinct) << "hash/equivalence in DISTINCT";
    }
  }

  // Orderability: mixed with plain lists, the embedding sorts as the list it materializes to.
  auto s = env.faker.Interpret(
      "MATCH (n:Doc) WITH n.emb AS e UNWIND [[0.5, 0.5, 0.5, 1.5], e, [0.5, 0.5, 0.5], "
      "[0.5, 0.5, 0.5, 0.5, 0.5], [0.5, 0.5, 0.5, 0.25]] AS x RETURN x ORDER BY x");
  const std::vector<std::vector<double>> sorted = {
      {0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 0.25}, {0.5, 0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 1.5}};
  EXPECT_EQ(EmbeddingRows(s), sorted);
}

// Guards IsPropertyValue(VectorRef): an equality seek on a label-property index with an embedding as the
// lookup value must find the node holding the equal list, with and without the index.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_EmbeddingAsIndexSeekValue) {
  QueryEnv env{data_dir_, "t17", 1, 4};
  auto &faker = env.faker;
  faker.Interpret("CREATE (:B {p: [0.5, 0.5, 0.5, 0.5]}), (:B {p: [0.5, 0.5, 0.5, 0.25]})");
  const std::string query = "MATCH (a:Doc) WITH a LIMIT 1 MATCH (b:B) WHERE b.p = a.emb RETURN count(b) AS c";

  EXPECT_EQ(faker.Interpret(query).GetResults().at(0)[0].ValueInt(), 1) << "without an index";

  faker.Interpret("CREATE INDEX ON :B(p)");
  std::string plan;
  for (const auto &row : faker.Interpret("EXPLAIN " + query).GetResults()) plan += row[0].ValueString() + "\n";
  ASSERT_NE(plan.find("ScanAllByLabelProperties"), std::string::npos) << plan;
  EXPECT_EQ(faker.Interpret(query).GetResults().at(0)[0].ValueInt(), 1) << "through the label-property index";
}

// The user-facing write path: SET copying an embedding to another property runs the ref through
// ToPropertyValue and must store the full vector rather than throwing "Unsupported conversion".
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_SetCopiesEmbeddingProperty) {
  auto gk = MakeDb("t10");
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(MakeConfig(data_dir_ / "t10"))};
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVertices, kDim);
  CreateIndex(db.get(), label, prop, kDim, kNumVertices);

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker faker{&interpreter_context, db};

  auto stream = faker.Interpret("MATCH (n) WITH n LIMIT 1 SET n.copy = n.emb RETURN size(n.copy) AS s");
  ASSERT_EQ(stream.GetResults().size(), 1u);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim))
      << "SET n.copy = n.emb must persist the full embedding as a list";
}

// A lazy embedding is a list; every list operation must treat it as one instead of throwing
// "got vector". Regression for the e2e durability failure, where `ORDER BY n.emb[0]` (a subscript on
// the embedding) threw and aborted the query.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_ListOperationsOnEmbedding) {
  auto gk = MakeDb("t11");
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(MakeConfig(data_dir_ / "t11"))};
  memgraph::dbms::DatabaseAccess db = [&]() {
    auto a = gk->access();
    MG_ASSERT(a, "db access");
    return *a;
  }();
  auto label = db->storage()->NameToLabel("Doc");
  auto prop = db->storage()->NameToProperty("emb");
  Populate(db.get(), label, prop, kNumVertices, kDim);
  CreateIndex(db.get(), label, prop, kDim, kNumVertices);

  memgraph::system::System system_state;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          kNoHandler,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };
  InterpreterFaker faker{&interpreter_context, db};

  // Subscript, in an ORDER BY key — the exact operation that threw on a VectorRef in the e2e test.
  {
    auto stream = faker.Interpret("MATCH (n) RETURN n.emb ORDER BY n.emb[0] LIMIT 1");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueList().size(), static_cast<size_t>(kDim));
  }
  // Bare subscript yields the element (embeddings are Populate()'d as (i%7)+0.5, so emb[0] is a double).
  {
    auto stream = faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN n.emb[0] AS x");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_TRUE(stream.GetResults()[0][0].IsDouble());
  }
  // size() and UNWIND treat the embedding as a list of kDim elements.
  {
    auto stream = faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN size(n.emb) AS s");
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
  }
  {
    auto stream = faker.Interpret("MATCH (n) WITH n LIMIT 1 UNWIND n.emb AS e RETURN count(e) AS c");
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
  }
  // Dynamic string subscript n["emb"] reads the same property; it must also come back as a full list
  // (and, like n.emb, be subscriptable) rather than an empty or unhandled value.
  {
    auto stream = faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN size(n[\"emb\"]) AS s, n[\"emb\"][0] AS x");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
    EXPECT_TRUE(stream.GetResults()[0][1].IsDouble());
  }
}

// Exercises the lazy-ref surfaces that only matter across threads, for a TSan build: parallel plans
// whose workers materialize shared lazy refs, and concurrent readers holding lazy refs across a
// sort/dedup while a writer overwrites the same embeddings.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_ConcurrentReadersWritersAndParallelPlans) {
  constexpr int kCount = 300;
  constexpr int kSmallDim = 8;
  QueryEnv env{data_dir_, "t18", kCount, kSmallDim};

#ifdef MG_ENTERPRISE
  {
    memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::ENTERPRISE);
    memgraph::utils::PriorityThreadPool pool{3, 1};
    env.interpreter_context.worker_pool = &pool;

    std::vector<double> expected_sorted;
    for (int i = 0; i < kCount; ++i) expected_sorted.push_back(static_cast<double>(i % 7) + 0.5);
    std::ranges::sort(expected_sorted);

    // Every returned vector is uniform with full dim; returns its common element.
    auto first_of = [&](const auto &v) {
      const auto &list = v.ValueList();
      EXPECT_EQ(list.size(), static_cast<size_t>(kSmallDim));
      for (const auto &x : list) EXPECT_EQ(x.ValueDouble(), list.front().ValueDouble());
      return list.front().ValueDouble();
    };
    auto expect_parallel = [&](const auto &stream, const std::string &query) {
      EXPECT_FALSE(stream.GetSummary().contains("notifications")) << "fell back to serial execution: " << query;
    };

    {
      const std::string q = "USING PARALLEL EXECUTION MATCH (n:Doc) RETURN n.emb ORDER BY n.emb";
      auto stream = env.faker.Interpret(q);
      expect_parallel(stream, q);
      ASSERT_EQ(stream.GetResults().size(), static_cast<size_t>(kCount));
      std::vector<double> got;
      for (const auto &row : stream.GetResults()) got.push_back(first_of(row[0]));
      EXPECT_EQ(got, expected_sorted);
    }
    {
      const std::string q = "USING PARALLEL EXECUTION MATCH (n:Doc) RETURN DISTINCT n.emb";
      auto stream = env.faker.Interpret(q);
      // The planner does not parallelize a bare DISTINCT; it runs serially, which still checks the result.
      std::vector<double> got;
      for (const auto &row : stream.GetResults()) got.push_back(first_of(row[0]));
      std::ranges::sort(got);
      EXPECT_EQ(got, (std::vector<double>{0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5}));
    }
    {
      const std::string q = "USING PARALLEL EXECUTION MATCH (n:Doc) RETURN count(DISTINCT n.emb)";
      auto stream = env.faker.Interpret(q);
      expect_parallel(stream, q);
      ASSERT_EQ(stream.GetResults().size(), 1u);
      EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), 7);
    }
    {
      const std::string q = "USING PARALLEL EXECUTION MATCH (n:Doc) RETURN collect(n.emb)";
      auto stream = env.faker.Interpret(q);
      expect_parallel(stream, q);
      ASSERT_EQ(stream.GetResults().size(), 1u);
      std::vector<double> got;
      for (const auto &v : stream.GetResults()[0][0].ValueList()) got.push_back(first_of(v));
      std::ranges::sort(got);
      EXPECT_EQ(got, expected_sorted);
    }
    {
      const std::string q = "USING PARALLEL EXECUTION MATCH (n:Doc) RETURN n.emb AS e, count(*) AS c ORDER BY e";
      auto stream = env.faker.Interpret(q);
      expect_parallel(stream, q);
      ASSERT_EQ(stream.GetResults().size(), 7u);
      int64_t total = 0;
      double prev = -1;
      for (const auto &row : stream.GetResults()) {
        const double e = first_of(row[0]);
        EXPECT_GT(e, prev);
        prev = e;
        total += row[1].ValueInt();
      }
      EXPECT_EQ(total, kCount);
    }
    env.interpreter_context.worker_pool = nullptr;
  }
#endif

  constexpr int kReaders = 3;
  constexpr int kReaderIterations = 20;
  constexpr int kWriterIterations = 40;

  auto write_query = [&](int iteration) {
    const std::string value = std::to_string(100 + iteration) + ".25";
    std::string list = "[";
    for (int i = 0; i < kSmallDim; ++i) list += (i ? "," : "") + value;
    list += "]";
    return "MATCH (n:Doc) WHERE id(n) % 5 = 0 SET n.emb = " + list;
  };

  std::vector<std::unique_ptr<InterpreterFaker>> reader_fakers;
  for (int i = 0; i < kReaders; ++i) {
    reader_fakers.push_back(std::make_unique<InterpreterFaker>(&env.interpreter_context, env.db));
  }
  InterpreterFaker writer_faker{&env.interpreter_context, env.db};

  std::atomic<int> violations{0};
  std::atomic<int> reader_errors{0};
  std::atomic<int> writer_conflicts{0};
  std::atomic<int> writes_committed{0};
  std::latch start{kReaders + 1};

  // True iff every vector has full dim with all elements equal; the value is returned through `out`.
  auto uniform = [&](const auto &v, double &out) {
    const auto &list = v.ValueList();
    if (list.size() != static_cast<size_t>(kSmallDim)) return false;
    out = list.front().ValueDouble();
    return std::ranges::all_of(list, [&](const auto &x) { return x.ValueDouble() == out; });
  };

  auto reader = [&](InterpreterFaker &faker) {
    start.arrive_and_wait();
    for (int it = 0; it < kReaderIterations; ++it) {
      try {
        {
          auto stream = faker.Interpret("MATCH (n:Doc) WITH n ORDER BY n.emb RETURN n.emb");
          if (stream.GetResults().size() != static_cast<size_t>(kCount)) violations.fetch_add(1);
          double prev = -1;
          for (const auto &row : stream.GetResults()) {
            double cur = 0;
            if (!uniform(row[0], cur) || cur < prev) violations.fetch_add(1);
            prev = cur;
          }
        }
        {
          auto stream = faker.Interpret("MATCH (n:Doc) RETURN DISTINCT n.emb");
          std::vector<double> seen;
          for (const auto &row : stream.GetResults()) {
            double cur = 0;
            if (!uniform(row[0], cur)) violations.fetch_add(1);
            seen.push_back(cur);
          }
          std::ranges::sort(seen);
          if (std::ranges::adjacent_find(seen) != seen.end()) violations.fetch_add(1);
        }
        {
          auto stream = faker.Interpret("MATCH (n:Doc) RETURN count(DISTINCT n.emb)");
          if (stream.GetResults().size() != 1 || stream.GetResults()[0][0].ValueInt() < 1 ||
              stream.GetResults()[0][0].ValueInt() > kCount) {
            violations.fetch_add(1);
          }
        }
      } catch (const std::exception &) {
        reader_errors.fetch_add(1);
      }
    }
  };

  auto writer = [&] {
    start.arrive_and_wait();
    for (int it = 0; it < kWriterIterations; ++it) {
      try {
        writer_faker.Interpret(write_query(it));
        writes_committed.fetch_add(1);
      } catch (const std::exception &) {
        writer_conflicts.fetch_add(1);
      }
    }
  };

  {
    std::vector<std::jthread> threads;
    for (auto &faker : reader_fakers) threads.emplace_back(reader, std::ref(*faker));
    threads.emplace_back(writer);
  }

  EXPECT_EQ(violations.load(), 0) << "a reader saw a torn, unsorted, duplicated or wrong-sized embedding";
  EXPECT_EQ(reader_errors.load(), 0);
  EXPECT_GT(writes_committed.load(), 0) << "writer never committed; conflicts=" << writer_conflicts.load();
}
