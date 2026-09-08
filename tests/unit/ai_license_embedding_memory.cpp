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
//
// Facts under test:
//   1. Embeddings live ONLY in the usearch index; the vertex property is replaced by a compact
//      VectorIndexId reference. Reading it reconstructs the full vector from the index.
//   2. That reconstruction is a plain operator-new allocation -> default arena -> graph_memory_tracker
//      (the AI_PLATFORM license limit), NOT the exempt vector_index_memory_tracker, and NOT the
//      per-DB query PMR tracker.
//   3. Under an AI_PLATFORM graph limit, retaining a whole result set of embeddings (RETURN n)
//      aborts, while reconstructing-and-discarding one at a time (count(n.emb)) stays under the cap,
//      even though the embedding index itself is far larger than the cap (it is exempt).

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <vector>

#include "dbms/database.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "memory/db_arena.hpp"
#include "memory/global_memory_control.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory_tracker.hpp"

using memgraph::storage::PropertyValue;
using memgraph::storage::View;

namespace {

// Small scale for the storage-model / query-semantics tests (fast).
constexpr int kDim = 256;
constexpr int kNumVertices = 1024;
// graph_memory_tracker is fed by jemalloc EXTENT-COMMIT hooks, so it moves only when committed arena
// memory grows past jemalloc's retained (dirty-page) slack. The accounting tests therefore use a
// working set large enough to force new extent commits after a purge.
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
    // Install the global graph arena hooks so plain operator-new allocations (the reconstructed
    // embedding small_vector, the retained result set) are attributed to graph_memory_tracker,
    // exactly as they are in a running server.
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

// ---------------------------------------------------------------------------
// 1. Storage model: after indexing, the embedding is gone from the property store (replaced by a
//    compact reference) and lives in the vector index. Reading reconstructs the full vector.
// ---------------------------------------------------------------------------
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

  // After indexing, the property is no longer a raw list: it is a compact VectorIndexId reference,
  // and reading it reconstructs the full vector out of the index. (The property-store shrink is real
  // but not observable via DbStorageMemoryUsage, which tracks committed arena extents, not logical
  // property bytes.)
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

// ---------------------------------------------------------------------------
// 2. Materialization accounting: reconstructing embeddings on read charges graph_memory_tracker
//    (the AI limit), leaves the exempt vector_index_memory_tracker untouched, and does NOT touch
//    the per-DB query PMR tracker. (Answers: is it missed? no. which tracker? graph.)
// ---------------------------------------------------------------------------
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
  // The exempt vector_index domain is not re-charged by reading, and the per-DB query PMR tracker
  // sees nothing: the reconstruction is a plain operator-new, not a PMR allocation. So "removing
  // query-runtime tracking from the AI limit" (the db_query PMR path) would NOT exempt it.
  EXPECT_LT(vidx_delta, retained_bytes / 8) << "The exempt vector_index domain is not re-charged by reading";
  EXPECT_LT(dbq_delta, retained_bytes / 8) << "Reconstruction is not query PMR memory";
  EXPECT_GT(graph_delta, vidx_delta * 4);
  EXPECT_GT(graph_delta, dbq_delta * 4);

  held.clear();
}

// ---------------------------------------------------------------------------
// 3. AI limit behaviour: with a graph limit far below the embedding index size, the index/embeddings
//    are fine (exempt), reconstruct-and-discard (count(n.emb)) survives, but retaining the whole set
//    (RETURN n) throws OutOfMemory. This is the customer symptom, reproduced.
// ---------------------------------------------------------------------------
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

  // Simulate an AI_PLATFORM license with this graph limit, through the real routing.
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

// ---------------------------------------------------------------------------
// 4. Query semantics: confirm the real interpreter reproduces the accessor-level model.
//    MATCH (n) RETURN n retains all rows (graph elevated while the stream is alive);
//    MATCH (n) RETURN count(n.emb) folds to one scalar (graph returns to baseline) yet still
//    reconstructs each embedding (count == kNumVertices proves every n.emb was evaluated non-null).
// ---------------------------------------------------------------------------
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

// ---------------------------------------------------------------------------
// 5. Double-charge: at rest the embedding lives in the exempt vector_index domain (counted in
//    total via vector_index). Materializing it adds a SECOND copy in the graph domain (also
//    counted in total). So total_memory_tracker carries the same logical embedding TWICE while a
//    result set is alive. Also refutes the earlier "reads route through the DB arena" hypothesis:
//    reconstruction is std::allocator -> default arena, so DbStorageMemoryUsage does NOT grow.
// ---------------------------------------------------------------------------
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
  // Reads use std::allocator -> default arena, NOT the DB arena: storage tracker stays flat.
  // (Refutes the 'query reads route through db_memory_tracker' double-count hypothesis.)
  EXPECT_LT(storage_delta, retained_bytes / 8) << "reconstruction does not touch the per-DB storage arena";

  held.clear();
}

#endif  // USE_JEMALLOC
