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

// AI_PLATFORM memory accounting for vector embeddings: reads reconstruct the vector via operator-new,
// charging graph_memory_tracker (AI limit), not the exempt vector_index domain.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <latch>
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

constexpr int kDim = 256;
constexpr int kNumVertices = 1024;
#if USE_JEMALLOC
// graph_memory_tracker moves only on fresh jemalloc extent commits, so the working set must exceed the dirty-page
// slack.
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
    memgraph::license::global_license_checker.DisableTesting();
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

  // Vertex i holds an embedding whose elements all equal (i % 7) + 0.5 (see Populate).
  struct QueryEnv {
    QueryEnv(const std::filesystem::path &root, const std::string &name, int count, int dim,
             memgraph::storage::IsolationLevel isolation = memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
             memgraph::storage::StorageMode mode = memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
             bool with_index = true)
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
      if (with_index) CreateIndex(db.get(), label, prop, dim, count);
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

class AiLicenseEmbeddingMemoryTest : public AiLicenseEmbeddingTest {
 protected:
  void SetUp() override {
    AiLicenseEmbeddingTest::SetUp();
    memgraph::memory::SetHooks();
  }
};

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

// With the graph cap below the (exempt) index size, reconstruct-and-discard survives but retaining all throws OOM.
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
  const int64_t headroom = 4 * kOneMiB;
  const int64_t limit = Graph() + headroom;
  ASSERT_GT(retained_bytes, 2 * headroom) << "test must retain far more than the headroom";

  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::AI_PLATFORM, limit);

  EXPECT_GT(embeddings_in_index, headroom)
      << "The embedding index is larger than the graph limit, yet coexists with it (it is exempt)";

  // The tracker only throws inside an enabler scope.
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;

  {
    auto racc = db->Access();
    EXPECT_NO_THROW({
      for (auto v : racc->Vertices(View::OLD)) {
        auto pv = v.GetProperty(prop, View::OLD);
        ASSERT_TRUE(pv.has_value());
        volatile auto sz = pv->ValueVectorIndexList().size();
        (void)sz;
      }
    });
  }

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

TEST_F(AiLicenseEmbeddingMemoryTest, QuerySemantics_ReturnNVsCountEmb) {
  QueryEnv env{data_dir_, "t4", kNumVerticesBig, kDimBig};

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);

  Stabilize(env.db.get());
  int64_t held_return = 0;
  {
    const int64_t before = Graph();
    auto stream = env.faker.Interpret("MATCH (n) RETURN n");
    held_return = Graph() - before;
    EXPECT_EQ(stream.GetResults().size(), static_cast<size_t>(kNumVerticesBig));
    EXPECT_GT(held_return, retained_bytes / 2)
        << "RETURN n retains the whole materialized result set (embeddings) in graph memory";
  }

  Stabilize(env.db.get());
  int64_t held_count = 0;
  {
    const int64_t before = Graph();
    auto stream = env.faker.Interpret("MATCH (n) RETURN count(n.emb)");
    held_count = Graph() - before;
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), kNumVerticesBig)
        << "count(n.emb) evaluated (reconstructed) every embedding";
  }

  EXPECT_GT(held_return, held_count * 4)
      << "RETURN n materializes far more embedding memory than count(n.emb): held_return=" << held_return
      << " held_count=" << held_count;
}

// The exempt index copy stays counted in total, so total carries the embedding twice while a materialized copy is live.
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
  const int64_t dbq_before = db->DbQueryMemoryUsage();

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
  const int64_t dbq_delta = db->DbQueryMemoryUsage() - dbq_before;

  EXPECT_EQ(held.size(), static_cast<size_t>(kNumVerticesBig));

  EXPECT_LT(vidx_delta, retained_bytes / 8) << "index copy is untouched by reading";
  EXPECT_GT(graph_delta, retained_bytes / 2) << "materialized copy lands in the graph domain";
  EXPECT_GT(total_delta, retained_bytes / 2) << "total_memory_tracker now carries the embedding twice";
  EXPECT_LT(storage_delta, retained_bytes / 8) << "reconstruction does not touch the per-DB storage arena";
  EXPECT_LT(dbq_delta, retained_bytes / 8) << "Reconstruction is not query PMR memory";
  EXPECT_GT(graph_delta, vidx_delta * 4);
  EXPECT_GT(graph_delta, dbq_delta * 4);

  held.clear();
}

// GetVectorInto reuses a scratch buffer, so a lazy sort's graph footprint is O(dim), not O(N*dim), and matches an eager
// sort.
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

  // Compare value sequences, not vertex order, to be robust to ties.
  std::vector<std::vector<float>> lazy_sorted;
  lazy_sorted.reserve(verts.size());
  for (auto &va : verts) {
    ASSERT_TRUE(va.GetVectorInto(prop, View::OLD, buf_a));
    lazy_sorted.emplace_back(buf_a.begin(), buf_a.end());
  }
  EXPECT_EQ(lazy_sorted, eager_sorted) << "lazy comparator must produce the same order as an eager sort";
}

// ORDER BY and DISTINCT stay within the AI graph cap; RETURN n.emb blows it (proves the cap is tight).
TEST_F(AiLicenseEmbeddingMemoryTest, Variant1Lazy_OrderByAndDistinctBoundedThroughInterpreter) {
  QueryEnv env{data_dir_, "t7", kNumVerticesBig, kDimBig};

  const int64_t retained_bytes = static_cast<int64_t>(kNumVerticesBig) * kDimBig * sizeof(float);
  // Headroom covers the O(N) VectorRef cache but not O(N*dim) materialization.
  const int64_t headroom = 8 * kOneMiB;
  Stabilize(env.db.get());
  const int64_t limit = Graph() + headroom;
  ASSERT_GT(retained_bytes, headroom + 2 * kOneMiB)
      << "materialising every embedding must exceed the cap, so (c) is a real abort";

  memgraph::license::global_license_checker.EnableTesting(memgraph::license::LicenseType::AI_PLATFORM, limit);
  memgraph::utils::MemoryTracker::OutOfMemoryExceptionEnabler enabler;

  // (a) LIMIT keeps the ordering from being optimised away.
  {
    auto stream = env.faker.Interpret("MATCH (n) WITH n ORDER BY n.emb LIMIT 1000000 RETURN count(n) AS c");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), kNumVerticesBig)
        << "every row was sorted by its embedding without exceeding the AI graph cap";
  }

  // (b) Populate yields 7 distinct vectors (i % 7). Purge so dirty-page slack from the prior query doesn't eat
  // headroom.
  memgraph::memory::PurgeUnusedMemory();
  {
    auto stream = env.faker.Interpret("MATCH (n) RETURN DISTINCT n.emb AS e");
    EXPECT_EQ(stream.GetResults().size(), 7u)
        << "DISTINCT over embeddings deduped via lazy hash/equality without exceeding the AI graph cap";
  }

  memgraph::memory::PurgeUnusedMemory();
  EXPECT_THROW(env.faker.Interpret("MATCH (n) RETURN n.emb"), memgraph::utils::OutOfMemoryException);
}

#endif  // USE_JEMALLOC

// `n{.*}` wraps the embedding as a lazy VectorRef; it must still materialise to the full list.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_MapProjectionMaterialisesFullEmbedding) {
  QueryEnv env{data_dir_, "t8", kNumVertices, kDim};

  auto stream = env.faker.Interpret("MATCH (n) RETURN n{.*} AS m");
  ASSERT_EQ(stream.GetResults().size(), static_cast<size_t>(kNumVertices));
  for (const auto &row : stream.GetResults()) {
    const auto &m = row[0].ValueMap();
    auto it = m.find("emb");
    ASSERT_NE(it, m.end()) << "n{.*} must include the embedding property";
    const auto &emb = it->second.ValueList();
    EXPECT_EQ(emb.size(), static_cast<size_t>(kDim)) << "lazy map embedding must materialise to full dimension";
  }
}

// A lazy VectorRef must hash like its materialised list (else DISTINCT/GROUP BY split them) and convert
// to a PropertyValue instead of throwing (so SET/CREATE can copy an embedding).
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

  memgraph::storage::PropertyValue pv;
  EXPECT_NO_THROW(pv = vref.ToPropertyValue(nullptr));
  const memgraph::query::TypedValue back{pv, nullptr, mem};
  ASSERT_TRUE(back.IsList());
  EXPECT_EQ(back.ValueList().size(), static_cast<size_t>(kDim));
}

// SetProperty updates usearch immediately, so usearch only holds the live value. A View::OLD ref materialized
// after a same-transaction overwrite must take the pre-SET floats from the delta, not usearch.
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

  // usearch now holds the 42.0 vector.
  const std::vector<double> replacement(kDim, 42.0);
  ASSERT_TRUE(sva.SetProperty(prop, PropertyValue(replacement)).has_value());

  const memgraph::query::TypedValue materialised = vref_old.MaterializeVectorRef(mem);
  ASSERT_TRUE(materialised.IsList());
  ASSERT_EQ(materialised.ValueList().size(), static_cast<size_t>(kDim))
      << "OLD-view lazy ref must materialize from the delta-carried floats";
  EXPECT_FLOAT_EQ(static_cast<float>(materialised.ValueList().front().ValueDouble()), 0.5F)
      << "materialized value must be the committed embedding, not the same-transaction overwrite";
}

// Another transaction's UNCOMMITTED SET already updated usearch; a reader must still get the committed
// floats from the delta chain, in both views and after the writer aborts.
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

// Write plans must stay eager (EvaluationContext::lazy_vector_refs): a lazy ref held across a rewrite/delete
// would re-read storage and miss the original vector.
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

// Lazy refs are only legal when IsSnapshotStable(); elsewhere reads stay eager with identical results.
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

// Interpreter::ThrowIfLazyVectorReaderOpen: a write must not run while an earlier read in the same explicit
// transaction still holds unconsumed lazy refs.
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

  // Without a vector index no lazy ref can exist, so the guard must not fire.
  QueryEnv plain{data_dir_,
                 "t15_noindex",
                 3,
                 8,
                 memgraph::storage::IsolationLevel::SNAPSHOT_ISOLATION,
                 memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
                 /*with_index=*/false};
  auto &plain_faker = plain.faker;
  plain_faker.interpreter.BeginTransaction();
  auto [r2, r2_qid] = plain_faker.Prepare("MATCH (n:Doc) RETURN n.emb");
  plain_faker.Pull(&r2, 1, r2_qid);
  ASSERT_TRUE(r2.GetSummary().at("has_more").ValueBool());
  auto [w2, w2_qid] = plain_faker.Prepare("CREATE (:Other)");
  EXPECT_NO_THROW(plain_faker.Pull(&w2, {}, w2_qid)) << "no vector index: partial read must not block a write";
  plain_faker.Abort();
}

// A VectorRef must compare, order, hash and test emptiness like its list. The embedding is [0.5, 0.5, 0.5, 0.5]
// (exact in float and double).
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

  auto s = env.faker.Interpret(
      "MATCH (n:Doc) WITH n.emb AS e UNWIND [[0.5, 0.5, 0.5, 1.5], e, [0.5, 0.5, 0.5], "
      "[0.5, 0.5, 0.5, 0.5, 0.5], [0.5, 0.5, 0.5, 0.25]] AS x RETURN x ORDER BY x");
  const std::vector<std::vector<double>> sorted = {
      {0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 0.25}, {0.5, 0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 0.5, 0.5}, {0.5, 0.5, 0.5, 1.5}};
  EXPECT_EQ(EmbeddingRows(s), sorted);

  // ref-vs-ref: vertex i holds (i % 7) + 0.5, so vertices 0 and 7 are equal and the rest differ.
  QueryEnv pair_env{data_dir_, "t16_pairs", 8, 4};
  for (const char *op : {"=", "<"}) {
    SCOPED_TRACE(op);
    const std::string o = op;
    auto refs = pair_env.faker.Interpret("MATCH (a:Doc), (b:Doc) WITH a.emb AS x, b.emb AS y WHERE x " + o +
                                         " y RETURN count(*) AS c");
    auto lists = pair_env.faker.Interpret(
        "MATCH (a:Doc), (b:Doc) WITH [i IN a.emb | i] AS x, [i IN b.emb | i] AS y "
        "WHERE x " +
        o + " y RETURN count(*) AS c");
    EXPECT_EQ(refs.GetResults().at(0)[0].ValueInt(), lists.GetResults().at(0)[0].ValueInt());
    EXPECT_EQ(refs.GetResults().at(0)[0].ValueInt(), o == "=" ? 10 : 27);
  }
}

// TypedValue::IsPropertyValue on a VectorRef: an index equality seek with an embedding as lookup value must work.
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

TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_SetCopiesEmbeddingProperty) {
  QueryEnv env{data_dir_, "t10", kNumVertices, kDim};

  auto stream = env.faker.Interpret("MATCH (n) WITH n LIMIT 1 SET n.copy = n.emb RETURN size(n.copy) AS s");
  ASSERT_EQ(stream.GetResults().size(), 1u);
  EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim))
      << "SET n.copy = n.emb must persist the full embedding as a list";
}

// Regression for the e2e durability failure: `ORDER BY n.emb[0]` threw "got vector" on a VectorRef.
TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_ListOperationsOnEmbedding) {
  QueryEnv env{data_dir_, "t11_list", kNumVertices, kDim};

  {
    auto stream = env.faker.Interpret("MATCH (n) RETURN n.emb ORDER BY n.emb[0] LIMIT 1");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueList().size(), static_cast<size_t>(kDim));
  }
  {
    auto stream = env.faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN n.emb[0] AS x");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_TRUE(stream.GetResults()[0][0].IsDouble());
  }
  {
    auto stream = env.faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN size(n.emb) AS s");
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
  }
  {
    auto stream = env.faker.Interpret("MATCH (n) WITH n LIMIT 1 UNWIND n.emb AS e RETURN count(e) AS c");
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
  }
  {
    auto stream = env.faker.Interpret("MATCH (n) WITH n LIMIT 1 RETURN size(n[\"emb\"]) AS s, n[\"emb\"][0] AS x");
    ASSERT_EQ(stream.GetResults().size(), 1u);
    EXPECT_EQ(stream.GetResults()[0][0].ValueInt(), static_cast<int64_t>(kDim));
    EXPECT_TRUE(stream.GetResults()[0][1].IsDouble());
  }
}

TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_PropertiesAndValuesStayListIdentical) {
  constexpr int kCount = 3;
  constexpr int kSmallDim = 8;
  QueryEnv env{data_dir_, "t20", kCount, kSmallDim};

  std::vector<std::vector<double>> expected;
  for (int i = 0; i < kCount; ++i) expected.emplace_back(kSmallDim, static_cast<double>(i) + 0.5);

  for (const char *expr : {"n.emb", "properties(n).emb", "values(n)[0]"}) {
    SCOPED_TRACE(expr);
    auto stream = env.faker.Interpret(std::string("MATCH (n:Doc) RETURN ") + expr + " ORDER BY n.emb[0]");
    EXPECT_EQ(EmbeddingRows(stream), expected);
  }
}

TEST_F(AiLicenseEmbeddingTest, Variant1Lazy_EdgeEmbeddingReadsThroughIndex) {
  QueryEnv env{data_dir_, "t21", 1, 4};
  env.faker.Interpret(
      "CREATE (:X)-[:E {emb: [3.0, 13.0, 23.0, 33.0]}]->(:X), (:X)-[:E {emb: [1.0, 11.0, 21.0, 31.0]}]->(:X), "
      "(:X)-[:E {emb: [2.0, 12.0, 22.0, 32.0]}]->(:X)");
  env.faker.Interpret(R"(CREATE VECTOR EDGE INDEX edge_emb ON :E(emb) WITH CONFIG {"dimension": 4, "capacity": 10})");

  const std::vector<std::vector<double>> expected = {
      {1.0, 11.0, 21.0, 31.0}, {2.0, 12.0, 22.0, 32.0}, {3.0, 13.0, 23.0, 33.0}};

  auto rows = EmbeddingRows(env.faker.Interpret("MATCH ()-[r:E]->() RETURN r.emb"));
  std::ranges::sort(rows);
  EXPECT_EQ(rows, expected);

  EXPECT_EQ(EmbeddingRows(env.faker.Interpret("MATCH ()-[r:E]->() RETURN r.emb ORDER BY r.emb[0]")), expected);

  auto stream = env.faker.Interpret("MATCH ()-[r:E]->() RETURN r.emb[0] AS x ORDER BY x");
  ASSERT_EQ(stream.GetResults().size(), 3u);
  for (size_t i = 0; i < 3; ++i) EXPECT_EQ(stream.GetResults()[i][0].ValueDouble(), static_cast<double>(i) + 1.0);
}

// For TSan: parallel plans materializing shared lazy refs, and readers holding lazy refs across sort/dedup
// while a writer overwrites the same embeddings.
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
      // The planner does not parallelize a bare DISTINCT, so no expect_parallel here.
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
