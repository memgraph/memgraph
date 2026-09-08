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
#include <cstdint>
#include <filesystem>
#include <span>
#include <utility>
#include <vector>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "memory/db_arena.hpp"
#include "memory/global_memory_control.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"

using memgraph::storage::PropertyValue;
using memgraph::storage::View;

namespace {

// Small scale for the storage-model / query-semantics tests (fast).
constexpr int kDim = 256;
constexpr int kNumVertices = 1024;
// graph_memory_tracker moves only when jemalloc commits new arena extents past its dirty-page slack.
// The accounting tests need a working set large enough to force fresh commits after a purge.
constexpr int kDimBig = 512;
constexpr int kNumVerticesBig = 6000;
constexpr int64_t kOneMiB = 1 << 20;
constexpr auto kNoHandler = nullptr;

memgraph::storage::Config MakeConfig(const std::filesystem::path &dir) {
  memgraph::storage::Config config{};
  config.durability.storage_directory = dir;
  config.disk.main_storage_directory = dir / "disk";
  config.gc.type = memgraph::storage::Config::Gc::Type::NONE;
  return config;
}

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

#if USE_JEMALLOC

class AiLicenseEmbeddingMemoryTest : public ::testing::Test {
 protected:
  std::filesystem::path data_dir_{std::filesystem::temp_directory_path() / "mg_test_ai_license_embedding"};

  void SetUp() override {
    std::filesystem::create_directories(data_dir_);
    // Install the global graph arena hooks so operator-new allocations (reconstructed embeddings,
    // retained result sets) are attributed to graph_memory_tracker, as in a running server.
    memgraph::memory::SetHooks();
  }

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
    const bool ok_x = x.GetVectorInto(prop, buf_a);
    const bool ok_y = y.GetVectorInto(prop, buf_b);
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
    ASSERT_TRUE(va.GetVectorInto(prop, buf_a));
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

// Map-projection round-trip: `n{.*}` wraps the compact VectorIndexId as a lazy VectorRef;
// Bolt serialisation must materialise the full embedding (a broken lazy path returns an empty list).
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1Lazy_MapProjectionMaterialisesFullEmbedding) {
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
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1Lazy_HashConsistentAndWritePathMaterialises) {
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
  const memgraph::query::TypedValue vref{memgraph::query::LazyVectorRef{sva, prop}, mem};
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

// The user-facing write path: SET copying an embedding to another property runs the ref through
// ToPropertyValue and must store the full vector rather than throwing "Unsupported conversion".
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1Lazy_SetCopiesEmbeddingProperty) {
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

#endif  // USE_JEMALLOC
