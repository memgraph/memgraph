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

// A recovered branch is the branch that was written, whatever was written. The randomized test drives seeded workloads
// against a model of the branch and requires live, recovered and model to agree; the others pin what recovery must
// get right on its own terms: a log it must refuse, a snapshot-isolated reader after a restart, the absence of marker
// ids from what a session reads, and edges recovered without any vertex stub.
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "dbms/database.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "versioning/branch_engine.hpp"
#include "versioning/version_store.hpp"
#include "versioning_recovery_support.hpp"

using namespace memgraph::tests::versioning_recovery;  // NOLINT(google-build-using-namespace)

using ::testing::HasSubstr;

namespace {
class VersioningRecoveryEquivalenceTest : public RecoveryTest {};

// What a session reads and the fork-delete counts: the part of the bookkeeping that replay must reproduce exactly. The
// filters and touched sets of a replay are a subset of the live ones (a write that changes nothing leaves no record).
std::string Visible(Captured captured) {
  captured.bookkeeping = captured.bookkeeping.substr(0, captured.bookkeeping.find('\n') + 1);
  return captured.Render();
}

// ---------------------------------------------------------------------------------------------------------------
// The randomized workload
// ---------------------------------------------------------------------------------------------------------------

struct ModelVertex {
  bool fork{false};
  bool alive{true};
  std::set<std::string> labels;
  std::map<std::string, int64_t> props;
};

struct ModelEdge {
  uint64_t gid{0};
  uint64_t from{0};
  uint64_t to{0};
  bool fork{false};
  bool alive{true};
  std::string type;
  std::map<std::string, int64_t> props;
};

// What the branch must read after every committed transaction, kept by the workload itself.
struct Model {
  std::map<uint64_t, ModelVertex> vertices;
  std::vector<ModelEdge> edges;
  uint64_t max_vertex_gid{0};
  uint64_t max_edge_gid{0};

  static Model Of(const Base &b) {
    Model m;
    m.vertices[b.v1.AsUint()] = {.fork = true, .labels = {"L", "M"}, .props = {{"p", 1}, {"q", 5}}};
    m.vertices[b.v2.AsUint()] = {.fork = true, .labels = {"M"}};
    m.vertices[b.v3.AsUint()] = {.fork = true, .labels = {"L"}};
    m.vertices[b.v4.AsUint()] = {.fork = true};
    m.edges.push_back({.gid = b.e1.AsUint(),
                       .from = b.v1.AsUint(),
                       .to = b.v2.AsUint(),
                       .fork = true,
                       .type = "T",
                       .props = {{"w", 1}}});
    m.edges.push_back({.gid = b.e2.AsUint(),
                       .from = b.v3.AsUint(),
                       .to = b.v1.AsUint(),
                       .fork = true,
                       .type = "T",
                       .props = {{"w", 2}}});
    m.max_vertex_gid = std::max({b.v1.AsUint(), b.v2.AsUint(), b.v3.AsUint(), b.v4.AsUint()});
    m.max_edge_gid = std::max(b.e1.AsUint(), b.e2.AsUint());
    return m;
  }

  bool EdgeVisible(const ModelEdge &e) const { return e.alive && vertices.at(e.from).alive && vertices.at(e.to).alive; }

  std::string Render() const {
    std::ostringstream out;
    const auto props = [](const std::map<std::string, int64_t> &p) {
      std::ostringstream o;
      for (const auto &[k, v] : p) o << k << "=" << v << ";";
      return o.str();
    };
    for (const auto &[gid, v] : vertices) {
      if (!v.alive) continue;
      out << "v" << gid << " [";
      for (const auto &label : v.labels) out << label << ",";
      out << "] {" << props(v.props) << "}\n";
      std::vector<const ModelEdge *> mine;
      for (const auto &e : edges) {
        if (e.from == gid && EdgeVisible(e)) mine.push_back(&e);
      }
      std::ranges::sort(mine, {}, &ModelEdge::gid);
      for (const auto *e : mine)
        out << "  e" << e->gid << " " << e->type << " -> v" << e->to << " {" << props(e->props) << "}\n";
    }
    return out.str();
  }
};

// The rendering of a Captured without its bookkeeping, comparable with Model::Render.
std::string VerticesOnly(Captured captured) {
  captured.bookkeeping.clear();
  return captured.Render();
}

// Names a user could type that resemble what a marker encoding might use.
const std::vector<std::string> kLabelPool = {
    "L", "M", "X", "NewL", "N", "__mg_removed_x", "__mg_deleted", "__mg_lit_x", "\xff\xfe_x"};
const std::vector<std::string> kPropPool = {"p", "q", "a", "__mg_removed_p", "__mg_deleted"};
const std::vector<std::string> kEdgePropPool = {"w", "a", "__mg_removed_w"};
const std::vector<std::string> kTypePool = {"T", "N"};

class Workload {
 public:
  Workload(Env &env, Model &model, std::mt19937_64 &rng) : e_(env), m_(model), rng_(rng) {}

  // One branch transaction of 1..5 operations, each of which changes something.
  void Transaction() {
    const auto ops = Pick(5) + 1;
    e_.Txn([&](mv::BranchContext &c, auto &diff) {
      c_ = &c;
      diff_ = &diff;
      for (uint64_t i = 0; i < ops; ++i) Operation();
    });
  }

 private:
  uint64_t Pick(uint64_t n) { return rng_() % n; }

  template <typename T>
  const T &PickFrom(const std::vector<T> &pool) {
    return pool[Pick(pool.size())];
  }

  std::vector<uint64_t> AliveVertices() const {
    std::vector<uint64_t> alive;
    for (const auto &[gid, v] : m_.vertices) {
      if (v.alive) alive.push_back(gid);
    }
    return alive;
  }

  std::optional<uint64_t> AnyVertex() {
    const auto alive = AliveVertices();
    return alive.empty() ? std::nullopt : std::optional{alive[Pick(alive.size())]};
  }

  ModelEdge *AnyEdge(bool fork_only = false) {
    std::vector<ModelEdge *> visible;
    for (auto &edge : m_.edges) {
      if (m_.EdgeVisible(edge) && (!fork_only || edge.fork)) visible.push_back(&edge);
    }
    return visible.empty() ? nullptr : visible[Pick(visible.size())];
  }

  ms::VertexAccessor Vertex(uint64_t gid) {
    return m_.vertices.at(gid).fork ? e_.Fork(ms::Gid::FromUint(gid))
                                    : *diff_->FindVertex(ms::Gid::FromUint(gid), ms::View::NEW);
  }

  ms::EdgeAccessor Edge(const ModelEdge &edge) {
    const auto gid = ms::Gid::FromUint(edge.gid);
    const auto from = ms::Gid::FromUint(edge.from);
    if (edge.fork) return e_.ForkEdge(gid, from);
    auto found = c_->FindDiffEdge(gid, from, ms::View::NEW);
    EXPECT_TRUE(found.has_value());
    return std::move(*found);
  }

  void Operation() {
    const auto roll = Pick(20);
    if (roll < 5) return ToggleLabel();
    if (roll < 9) return WriteProperty();
    if (roll < 11) return WriteEdgeProperty();
    if (roll < 13) return CreateNativeVertex();
    if (roll < 15) return CreateNativeEdge();
    if (roll == 15) return DeleteNativeEdge();
    if (roll == 16) return DeleteNativeVertex();
    if (roll == 17) return DeleteForkEdge();
    if (roll == 18) return DeleteForkVertex();
    return WriteProperty();
  }

  void ToggleLabel() {
    const auto gid = AnyVertex();
    if (!gid) return;
    auto &model = m_.vertices.at(*gid);
    const auto &name = PickFrom(kLabelPool);
    auto vertex = Vertex(*gid);
    const bool had = model.labels.contains(name);
    auto changed = had ? c_->RemoveLabel(vertex, e_.Label(name)) : c_->AddLabel(vertex, e_.Label(name));
    ASSERT_TRUE(changed.has_value() && *changed) << (had ? "removing " : "adding ") << name << " on v" << *gid;
    if (had) {
      model.labels.erase(name);
    } else {
      model.labels.insert(name);
    }
  }

  // Sets a key to a value it does not have, or removes it when it has one.
  template <typename Writer>
  void WriteKey(std::map<std::string, int64_t> &props, const std::vector<std::string> &pool, Writer &&write) {
    const auto &key = PickFrom(pool);
    const auto it = props.find(key);
    if (it != props.end() && Pick(2) == 0) {
      write(key, ms::PropertyValue{});
      props.erase(key);
      return;
    }
    int64_t value = static_cast<int64_t>(Pick(1000)) + 1;
    if (it != props.end() && it->second == value) ++value;
    write(key, ms::PropertyValue(value));
    props[key] = value;
  }

  void WriteProperty() {
    const auto gid = AnyVertex();
    if (!gid) return;
    auto vertex = Vertex(*gid);
    WriteKey(m_.vertices.at(*gid).props, kPropPool, [&](const std::string &key, ms::PropertyValue value) {
      ASSERT_TRUE(c_->SetProperty(vertex, e_.Prop(key), std::move(value)).has_value()) << key << " on v" << *gid;
    });
  }

  void WriteEdgeProperty() {
    auto *edge = AnyEdge();
    if (edge == nullptr) return;
    auto accessor = Edge(*edge);
    WriteKey(edge->props, kEdgePropPool, [&](const std::string &key, ms::PropertyValue value) {
      ASSERT_TRUE(c_->SetProperty(accessor, e_.Prop(key), std::move(value)).has_value()) << key << " on e" << edge->gid;
    });
  }

  void CreateNativeVertex() {
    auto vertex = c_->CreateNativeVertex();
    const auto gid = vertex.Gid().AsUint();
    ASSERT_GT(gid, m_.max_vertex_gid) << "a new native vertex takes a gid nobody used";
    m_.max_vertex_gid = gid;
    auto &model = m_.vertices[gid];
    if (Pick(2) == 0) {
      const auto &name = PickFrom(kLabelPool);
      ASSERT_TRUE(*c_->AddLabel(vertex, e_.Label(name)));
      model.labels.insert(name);
    }
    if (Pick(2) == 0) {
      WriteKey(model.props, kPropPool, [&](const std::string &key, ms::PropertyValue value) {
        ASSERT_TRUE(c_->SetProperty(vertex, e_.Prop(key), std::move(value)).has_value());
      });
    }
  }

  // Between any two vertices, fork ones through their stubs: the edge is native either way.
  void CreateNativeEdge() {
    const auto alive = AliveVertices();
    if (alive.size() < 2) return;
    const auto first = Pick(alive.size());
    const auto from = alive[first];
    const auto to = alive[(first + 1 + Pick(alive.size() - 1)) % alive.size()];
    auto from_stub = e_.Stub(ms::Gid::FromUint(from));
    auto to_stub = e_.Stub(ms::Gid::FromUint(to));
    const auto &type = PickFrom(kTypePool);
    auto edge = c_->CreateNativeEdge(from_stub, to_stub, e_.EdgeType(type));
    ASSERT_TRUE(edge.has_value());
    ASSERT_GT(edge->Gid().AsUint(), m_.max_edge_gid) << "a new native edge takes a gid nobody used";
    m_.max_edge_gid = edge->Gid().AsUint();
    ModelEdge model{.gid = edge->Gid().AsUint(), .from = from, .to = to, .type = type};
    if (Pick(3) == 0) {
      WriteKey(model.props, kEdgePropPool, [&](const std::string &key, ms::PropertyValue value) {
        ASSERT_TRUE(c_->SetProperty(*edge, e_.Prop(key), std::move(value)).has_value());
      });
    }
    m_.edges.push_back(std::move(model));
  }

  void RemoveNativeEdge(ModelEdge &edge) {
    auto accessor = Edge(edge);
    ASSERT_TRUE(diff_->DeleteEdge(&accessor).has_value());
    edge.alive = false;
  }

  void DeleteNativeEdge() {
    std::vector<ModelEdge *> natives;
    for (auto &edge : m_.edges) {
      if (!edge.fork && m_.EdgeVisible(edge)) natives.push_back(&edge);
    }
    if (!natives.empty()) RemoveNativeEdge(*natives[Pick(natives.size())]);
  }

  // The native edges on a vertex go first: a native vertex cannot be deleted with edges, and a fork vertex refuses
  // while a branch-created edge is attached.
  void DeleteNativeEdgesOf(uint64_t gid) {
    for (auto &edge : m_.edges) {
      if (!edge.fork && m_.EdgeVisible(edge) && (edge.from == gid || edge.to == gid)) RemoveNativeEdge(edge);
    }
  }

  void DeleteNativeVertex() {
    std::vector<uint64_t> natives;
    for (const auto &[gid, v] : m_.vertices) {
      if (!v.fork && v.alive) natives.push_back(gid);
    }
    if (natives.empty()) return;
    const auto gid = natives[Pick(natives.size())];
    DeleteNativeEdgesOf(gid);
    auto vertex = Vertex(gid);
    ASSERT_TRUE(diff_->DeleteVertex(&vertex).has_value());
    m_.vertices.at(gid).alive = false;
  }

  void DeleteForkEdge() {
    auto *edge = AnyEdge(/*fork_only=*/true);
    if (edge == nullptr) return;
    auto accessor = Edge(*edge);
    ASSERT_TRUE(*c_->DeleteForkEdge(accessor));
    edge->alive = false;
  }

  void DeleteForkVertex() {
    std::vector<uint64_t> forks;
    for (const auto &[gid, v] : m_.vertices) {
      if (v.fork && v.alive) forks.push_back(gid);
    }
    if (forks.empty()) return;
    const auto gid = forks[Pick(forks.size())];
    DeleteNativeEdgesOf(gid);
    ASSERT_TRUE(*c_->DeleteForkVertex(ms::Gid::FromUint(gid)));
    m_.vertices.at(gid).alive = false;
  }

  Env &e_;
  Model &m_;
  std::mt19937_64 &rng_;
  mv::BranchContext *c_{nullptr};
  ms::Storage::Accessor *diff_{nullptr};
};

uint64_t SeedFromEnvironment(uint64_t fallback) {
  const char *text = std::getenv("VERSIONING_RECOVERY_SEED");
  return text == nullptr ? fallback : std::strtoull(text, nullptr, 10);
}

// Phase 1 writes a workload on a durable branch, phase 2 recovers it and checks it, writes more (new objects must take
// fresh gids), and phase 3 recovers everything once more. In each phase the branch must read as the model says, and
// the recovered branch must read exactly as the live one did, fork-delete counts included.
void RunSeed(uint64_t seed) {
  SCOPED_TRACE("seed " + std::to_string(seed) + " (rerun with VERSIONING_RECOVERY_SEED=" + std::to_string(seed) + ")");
  std::mt19937_64 rng{seed};
  Base base{};
  Model model;
  std::string live;
  std::string main_before;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    model = Model::Of(base);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    Workload workload{env, model, rng};
    const auto transactions = 6 + rng() % 5;
    for (uint64_t i = 0; i < transactions; ++i) {
      workload.Transaction();
      ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "transaction " << i;
    }
    const auto captured = Capture(db, *ctx, base);
    ASSERT_EQ(VerticesOnly(captured), model.Render()) << "the live branch differs from the model";
    live = Visible(captured);
    main_before = DumpMain(db);
  }

  std::string recovered;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    const auto captured = Capture(db, *ctx, base);
    recovered = Visible(captured);
    ASSERT_EQ(recovered, live) << "the recovered branch differs from the live one";
    ASSERT_EQ(VerticesOnly(captured), model.Render());
    ASSERT_EQ(DumpMain(db), main_before) << "branch writes reached main";

    // New objects after a restart take gids no object of the branch, live or deleted, ever had.
    const auto main_vertex = NewMainVertex(db);
    ASSERT_TRUE(main_vertex.has_value());
    ASSERT_GT(main_vertex->AsUint(), model.max_vertex_gid);
    model.max_vertex_gid = main_vertex->AsUint();
    Env env{db, *ctx, base};
    Workload workload{env, model, rng};
    for (int i = 0; i < 3; ++i) {
      workload.Transaction();
      ASSERT_FALSE(::testing::Test::HasFatalFailure()) << "transaction after the restart " << i;
    }
    const auto after = Capture(db, *ctx, base);
    ASSERT_EQ(VerticesOnly(after), model.Render());
    live = Visible(after);
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  const auto captured = Capture(db, *ctx, base);
  ASSERT_EQ(Visible(captured), live) << "the branch recovered across two restarts differs from the live one";
  ASSERT_EQ(VerticesOnly(captured), model.Render());
}
}  // namespace

class RandomizedRecoveryTest : public VersioningRecoveryEquivalenceTest,
                               public ::testing::WithParamInterface<uint64_t> {};

TEST_P(RandomizedRecoveryTest, RecoveredBranchReadsAsTheLiveOneAndTheModelDo) {
  RunSeed(SeedFromEnvironment(GetParam()));
}

INSTANTIATE_TEST_SUITE_P(Seeds, RandomizedRecoveryTest, ::testing::Range<uint64_t>(1, 41));

// ---- Edges recovered with stubs only where a deleted edge's markers are
// -------------------------------------------------------------------------

// A fork edge property write is kept on the edge's own stub, and a delete adds marker entries to the stubs of its two
// endpoints: replaying them attaches no stub to any other vertex, and the diff engine holds no vertex of its own.
TEST_F(VersioningRecoveryEquivalenceTest, ForkEdgeWritesRecoverWithStubsOnlyAtTheEndpointsOfADeletedEdge) {
  Base base{};
  const auto expect_stubs = [&](mv::BranchContext &ctx, std::set<uint64_t> expected) {
    auto diff = ctx.diff_engine().Access(ms::READ);
    ASSERT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    std::set<uint64_t> stubs;
    for (const auto gid : {base.v1, base.v2, base.v3, base.v4}) {
      if (ctx.HasVertexStub(gid)) stubs.insert(gid.AsUint());
    }
    EXPECT_EQ(stubs, expected);
    EXPECT_EQ(diff->ApproximateVertexCount(), 0u);
    ctx.EndTransaction();
  };
  std::string live;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto e1 = env.ForkEdge(base.e1, base.v1);
      ASSERT_TRUE(c.SetProperty(e1, env.Prop("w"), ms::PropertyValue(5)).has_value());
      ASSERT_TRUE(c.SetProperty(e1, env.Prop("a"), ms::PropertyValue(6)).has_value());
    });
    expect_stubs(*ctx, {});
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto e2 = env.ForkEdge(base.e2, base.v3);
      ASSERT_TRUE(*c.DeleteForkEdge(e2));
    });
    expect_stubs(*ctx, {base.v1.AsUint(), base.v3.AsUint()});
    live = Visible(Capture(db, *ctx, base));
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  const auto recovered = Capture(db, *ctx, base);
  EXPECT_EQ(Visible(recovered), live);
  ASSERT_EQ(recovered.At(base.v1).out.size(), 1u);
  EXPECT_EQ(recovered.At(base.v1).out[0].props.at("w"), "5");
  EXPECT_EQ(recovered.At(base.v1).out[0].props.at("a"), "6");
  EXPECT_TRUE(recovered.At(base.v3).out.empty());
  EXPECT_THAT(recovered.bookkeeping, HasSubstr("deletes v=0 e=1"));
  expect_stubs(*ctx, {base.v1.AsUint(), base.v3.AsUint()});
}

// ---- Deleted fork edges, live and recovered -------------------------------------------------------------------------

// A deleted fork edge is a marker entry in the adjacency of both endpoints; replay writes the committed ones again
// through the same writers, and an aborted delete leaves none live. A live branch and a recovered one must hide the
// same edges: per vertex and direction, the markers the stub holds and what an expansion still gives.
TEST_F(VersioningRecoveryEquivalenceTest, RecoveredBranchHidesTheSameDeletedForkEdgesAsTheLiveOne) {
  Base base{};
  const auto hidden_and_expanded = [&](mv::BranchContext &ctx) {
    auto diff = ctx.diff_engine().Access(ms::READ);
    EXPECT_TRUE(ctx.BeginTransaction(diff.get()).has_value());
    std::ostringstream out;
    for (const auto gid : {base.v1, base.v2, base.v3, base.v4}) {
      if (!ctx.FindForkVertex(gid) || ctx.IsVertexDeleted(gid, ms::View::NEW)) {
        out << "v" << gid.AsUint() << " none\n";
        continue;
      }
      auto stub = ctx.FindStub(gid, ms::View::NEW);
      for (const auto direction : {ms::EdgeDirection::OUT, ms::EdgeDirection::IN}) {
        const size_t hidden = stub ? ctx.ReadStubAdjacency(stub->vertex_, direction, ms::View::NEW).hidden.size() : 0;
        out << "v" << gid.AsUint() << (direction == ms::EdgeDirection::OUT ? " out " : " in ") << hidden << " "
            << ctx.ResolveEdges(gid, direction, ms::View::NEW).size() << "\n";
      }
    }
    ctx.EndTransaction();
    return out.str();
  };
  const auto row = [](ms::Gid gid, const char *direction, size_t hidden, size_t expanded) {
    return "v" + std::to_string(gid.AsUint()) + " " + direction + " " + std::to_string(hidden) + " " +
           std::to_string(expanded) + "\n";
  };
  std::string live;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    {
      auto diff = ctx->diff_engine().Access(ms::WRITE);
      ASSERT_TRUE(ctx->BeginTransaction(diff.get()).has_value());
      ASSERT_TRUE(*ctx->DeleteForkEdge(env.ForkEdge(base.e2, base.v3)));
      ctx->EndTransaction();
      diff->Abort();
    }
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto edge = env.ForkEdge(base.e1, base.v1);
      ASSERT_TRUE(c.SetProperty(edge, Mem(db)->NameToProperty("w"), ms::PropertyValue(7)).has_value());
      ASSERT_TRUE(*c.DeleteForkEdge(edge));
    });
    live = hidden_and_expanded(*ctx);
    EXPECT_THAT(live, HasSubstr(row(base.v1, "out", 1, 0)));
    EXPECT_THAT(live, HasSubstr(row(base.v2, "in", 1, 0)));
    EXPECT_THAT(live, HasSubstr(row(base.v1, "in", 0, 1))) << "the edge of the aborted delete is still there";
    env.Txn([&](mv::BranchContext &c, auto &) { ASSERT_TRUE(*c.DeleteForkVertex(base.v1)); });
    live = hidden_and_expanded(*ctx);
    EXPECT_THAT(live, HasSubstr(row(base.v3, "out", 1, 0)));
    EXPECT_THAT(live, HasSubstr(row(base.v2, "in", 1, 0)));
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(hidden_and_expanded(*ctx), live);
}

// A stub under a mirrored (L, p) key holds the key's other part as copied base state. Replaying the branch's WAL runs
// the same writes, so the diff engine's index holds the same stubs live and recovered.
TEST_F(VersioningRecoveryEquivalenceTest, IndexedStubsAreTheSameLiveAndRecovered) {
  Base base{};
  const auto indexed_stubs = [&](memgraph::dbms::Database &db, mv::BranchContext &ctx) {
    auto &st = *db.storage();
    auto diff = ctx.diff_engine().Access(ms::READ);
    std::set<uint64_t> gids;
    for (auto v :
         diff->Vertices(st.NameToLabel("L"), std::vector<ms::PropertyPath>{st.NameToProperty("p")}, ms::View::NEW)) {
      gids.insert(v.Gid().AsUint());
    }
    return gids;
  };
  std::set<uint64_t> live_stubs;
  std::string live;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    {
      auto acc = db.storage()->UniqueAccess();
      ASSERT_TRUE(acc->CreateIndex(db.storage()->NameToLabel("L"), {db.storage()->NameToProperty("p")}).has_value());
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {  // v1 has L: copied
      auto v1 = env.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, env.Prop("p"), ms::PropertyValue(2)).has_value());
    });
    env.Txn([&](mv::BranchContext &c, auto &) {  // v3 has L and no p
      auto v3 = env.Fork(base.v3);
      ASSERT_TRUE(c.SetProperty(v3, env.Prop("p"), ms::PropertyValue(7)).has_value());
    });
    env.Txn([&](mv::BranchContext &c, auto &) {  // v2 has p only after this; L is new to it
      auto v2 = env.Fork(base.v2);
      ASSERT_TRUE(c.SetProperty(v2, env.Prop("p"), ms::PropertyValue(9)).has_value());
      ASSERT_TRUE(c.AddLabel(v2, env.Label("L")).has_value());
    });
    live_stubs = indexed_stubs(db, *ctx);
    ASSERT_EQ(live_stubs, (std::set<uint64_t>{base.v1.AsUint(), base.v2.AsUint(), base.v3.AsUint()}));
    live = Visible(Capture(db, *ctx, base));
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  EXPECT_EQ(indexed_stubs(db, *ctx), live_stubs);
  EXPECT_EQ(Visible(Capture(db, *ctx, base)), live);
}

// Every writer that moves a key leaves the diff engine's index as the same replay re-derives it: a map update that
// touches two keys, label-first and property-first orders, an init, a set back to the fork value and the removal of
// one part.
TEST_F(VersioningRecoveryEquivalenceTest, EveryKeyChangingWriterLeavesTheSameIndexLiveAndRecovered) {
  Base base{};
  using Keys = std::map<uint64_t, std::vector<ms::PropertyValue>>;
  const auto keys_of = [](memgraph::dbms::Database &db,
                          mv::BranchContext &ctx,
                          std::string_view label,
                          std::vector<std::string_view>
                              properties) {
    auto &st = *db.storage();
    auto diff = ctx.diff_engine().Access(ms::READ);
    std::vector<ms::PropertyId> ids;
    for (auto const property : properties) ids.push_back(st.NameToProperty(property));
    std::vector<ms::PropertyPath> paths(ids.begin(), ids.end());
    Keys keys;
    for (auto v : diff->Vertices(st.NameToLabel(label), paths, ms::View::NEW)) {
      auto &key = keys[v.Gid().AsUint()];
      for (auto const id : ids) key.push_back(*v.GetProperty(id, ms::View::NEW));
    }
    return keys;
  };
  const auto both = [&](memgraph::dbms::Database &db, mv::BranchContext &ctx) {
    return std::pair{keys_of(db, ctx, "L", {"p", "q"}), keys_of(db, ctx, "M", {"q"})};
  };
  std::pair<Keys, Keys> live_keys;
  std::string live;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    {
      auto acc = db.storage()->UniqueAccess();
      ASSERT_TRUE(acc->CreateIndex(db.storage()->NameToLabel("L"),
                                   {db.storage()->NameToProperty("p"), db.storage()->NameToProperty("q")})
                      .has_value());
      Commit(*acc);
    }
    {
      auto acc = db.storage()->UniqueAccess();
      ASSERT_TRUE(acc->CreateIndex(db.storage()->NameToLabel("M"), {db.storage()->NameToProperty("q")}).has_value());
      Commit(*acc);
    }
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    const auto p = env.Prop("p"), q = env.Prop("q");
    const auto update = [&](ms::VertexAccessor &v, std::map<ms::PropertyId, ms::PropertyValue> map) {
      ASSERT_TRUE(ctx->UpdateProperties(v, map).has_value());
    };
    // v1 is {L, M} p=1 q=5: a map touching both keys, then back to the fork's p, then q removed and re-set.
    env.Txn([&](mv::BranchContext &, auto &) {
      auto v1 = env.Fork(base.v1);
      update(v1, {{p, ms::PropertyValue(2)}, {q, ms::PropertyValue(6)}});
    });
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = env.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, p, ms::PropertyValue(1)).has_value());
    });
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = env.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, q, ms::PropertyValue()).has_value());
    });
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = env.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, q, ms::PropertyValue(8)).has_value());
    });
    // v2 is {M}: the label first, then the map.
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v2 = env.Fork(base.v2);
      ASSERT_TRUE(c.AddLabel(v2, env.Label("L")).has_value());
      update(v2, {{p, ms::PropertyValue(3)}, {q, ms::PropertyValue(4)}});
    });
    // v3 is {L} without properties: an init.
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v3 = env.Fork(base.v3);
      std::map<ms::PropertyId, ms::PropertyValue> map{{p, ms::PropertyValue(7)}, {q, ms::PropertyValue(9)}};
      ASSERT_TRUE(*c.InitProperties(v3, map));
      ASSERT_TRUE(c.AddLabel(v3, env.Label("M")).has_value());
    });
    // v4 is bare: the properties first, then both labels.
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v4 = env.Fork(base.v4);
      update(v4, {{p, ms::PropertyValue(10)}, {q, ms::PropertyValue(11)}});
      ASSERT_TRUE(c.AddLabel(v4, env.Label("M")).has_value());
      ASSERT_TRUE(c.AddLabel(v4, env.Label("L")).has_value());
    });
    live_keys = both(db, *ctx);
    const auto ints = [](std::initializer_list<int64_t> values) {
      std::vector<ms::PropertyValue> out;
      for (auto const value : values) out.emplace_back(value);
      return out;
    };
    ASSERT_EQ(live_keys.first,
              (Keys{{base.v1.AsUint(), ints({1, 8})},
                    {base.v2.AsUint(), ints({3, 4})},
                    {base.v3.AsUint(), ints({7, 9})},
                    {base.v4.AsUint(), ints({10, 11})}}));
    ASSERT_EQ(live_keys.second,
              (Keys{{base.v1.AsUint(), ints({8})},
                    {base.v2.AsUint(), ints({4})},
                    {base.v3.AsUint(), ints({9})},
                    {base.v4.AsUint(), ints({11})}}));
    live = Visible(Capture(db, *ctx, base));
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  const auto recovered = both(db, *ctx);
  EXPECT_EQ(recovered.first, live_keys.first);
  EXPECT_EQ(recovered.second, live_keys.second);
  EXPECT_EQ(Visible(Capture(db, *ctx, base)), live);
}

// ---- No marker id reaches a session ---------------------------------------------------------------------------------

// What `SHOW NODE LABELS` and a property listing are made of is what the branch reports for its vertices: only the
// user's labels and keys, by their own ids, however the fork objects were changed before the restart.
TEST_F(VersioningRecoveryEquivalenceTest, RecoveredBranchReportsOnlyUserLabelsAndKeys) {
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = env.Fork(base.v1);
      ASSERT_TRUE(*c.RemoveLabel(v1, env.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v1, env.Label("X")));
      ASSERT_TRUE(c.SetProperty(v1, env.Prop("p"), ms::PropertyValue()).has_value());
      ASSERT_TRUE(c.SetProperty(v1, env.Prop("a"), ms::PropertyValue(3)).has_value());
      auto v3 = env.Fork(base.v3);
      ASSERT_TRUE(*c.RemoveLabel(v3, env.Label("L")));
      ASSERT_TRUE(*c.AddLabel(v3, env.Label("L")));
      ASSERT_TRUE(*c.DeleteForkVertex(base.v2));
      auto n = c.CreateNativeVertex();
      ASSERT_TRUE(*c.AddLabel(n, env.Label("N")));
      ASSERT_TRUE(*c.AddLabel(n, env.Label("__mg_removed_L")));
      auto e1 = env.ForkEdge(base.e2, base.v3);
      ASSERT_TRUE(c.SetProperty(e1, env.Prop("w"), ms::PropertyValue()).has_value());
    });
  }
  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto ctx = Checkout(db, "b");
  ASSERT_NE(ctx, nullptr);
  std::set<uint32_t> label_ids;
  std::set<uint32_t> property_ids;
  {
    auto diff = ctx->diff_engine().Access(ms::READ);
    ASSERT_TRUE(ctx->BeginTransaction(diff.get()).has_value());
    for (auto v : ctx->Vertices(ms::View::NEW)) {
      for (const auto label : *ctx->Labels(v, ms::View::NEW)) label_ids.insert(label.AsUint());
      for (const auto &[pid, value] : *ctx->Properties(v, ms::View::NEW)) property_ids.insert(pid.AsUint());
      for (const auto &edge : *ctx->OutEdges(v, ms::View::NEW)) {
        for (const auto &[pid, value] : *ctx->Properties(edge, ms::View::NEW)) property_ids.insert(pid.AsUint());
      }
    }
    ctx->EndTransaction();
  }
  std::set<std::string> label_names;
  for (const auto id : label_ids) {
    EXPECT_FALSE(ms::IsReservedId(id)) << "label id " << id;
    if (!ms::IsReservedId(id)) label_names.insert(db.storage()->LabelToName(ms::LabelId::FromUint(id)));
  }
  for (const auto id : property_ids) EXPECT_FALSE(ms::IsReservedId(id)) << "property id " << id;
  EXPECT_EQ(Joined(label_names), "L,M,N,X,__mg_removed_L,");
  EXPECT_FALSE(property_ids.empty());
}

// ---- Snapshot isolation across a restart
// ------------------------------------------------------------------------------

// A reader that starts after the restart sees all the recovered transactions and keeps seeing exactly that while a
// writer commits and while main moves on; a reader that starts later sees the writer's commits and still reads main as
// of the fork.
TEST_F(VersioningRecoveryEquivalenceTest, ReaderAfterARestartKeepsItsSnapshotWhileAWriterCommits) {
  Base base{};
  std::string recovered_state;
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    ASSERT_TRUE(db.version_store()->CreateBranch("b").has_value());
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    for (int i = 1; i <= 3; ++i) {
      env.Txn([&](mv::BranchContext &c, auto &) {
        auto v1 = env.Fork(base.v1);
        ASSERT_TRUE(c.SetProperty(v1, env.Prop("p"), ms::PropertyValue(100 + i)).has_value());
        auto n = c.CreateNativeVertex();
        ASSERT_TRUE(c.SetProperty(n, env.Prop("a"), ms::PropertyValue(i)).has_value());
      });
    }
    recovered_state = VerticesOnly(Capture(db, *ctx, base));
  }

  memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/true)};
  const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
  auto reader = Checkout(db, "b");
  auto writer = Checkout(db, "b");
  ASSERT_NE(reader, nullptr);
  ASSERT_NE(writer, nullptr);

  const auto read_open = [&](mv::BranchContext &ctx) {
    Captured captured;
    captured.vertices = ReadVertices(db, ctx);
    return captured.Render();
  };

  auto reader_txn = reader->diff_engine().Access(ms::READ);
  ASSERT_TRUE(reader->BeginTransaction(reader_txn.get()).has_value());
  const auto first = read_open(*reader);
  EXPECT_EQ(first, recovered_state) << "a reader after the restart sees every recovered transaction";

  Env writer_env{db, *writer, base};
  std::optional<ms::Gid> main_vertex;
  for (int i = 0; i < 3; ++i) {
    writer_env.Txn([&](mv::BranchContext &c, auto &) {
      auto v1 = writer_env.Fork(base.v1);
      ASSERT_TRUE(c.SetProperty(v1, writer_env.Prop("p"), ms::PropertyValue(900 + i)).has_value());
      auto n = c.CreateNativeVertex();
      ASSERT_TRUE(c.SetProperty(n, writer_env.Prop("a"), ms::PropertyValue(50 + i)).has_value());
      if (i == 1) {
        auto v3 = writer_env.Fork(base.v3);
        ASSERT_TRUE(*c.RemoveLabel(v3, writer_env.Label("L")));
        auto e1 = writer_env.ForkEdge(base.e1, base.v1);
        ASSERT_TRUE(*c.DeleteForkEdge(e1));
        ASSERT_TRUE(*c.DeleteForkVertex(base.v4));
      }
    });
    EXPECT_EQ(read_open(*reader), first) << "commit " << i << " of the writer must not show in the open snapshot";
    if (i == 1) {  // main moves on: the fork state does not
      auto acc = db.Access(ms::WRITE);
      auto v2 = acc->FindVertex(base.v2, ms::View::OLD);
      ASSERT_TRUE(v2.has_value());
      ASSERT_TRUE(v2->SetProperty(writer_env.Prop("p"), ms::PropertyValue(555)).has_value());
      main_vertex = acc->CreateVertex().Gid();
      Commit(*acc);
    }
    EXPECT_EQ(read_open(*reader), first) << "a commit of main must not show in the open snapshot";
  }
  reader->EndTransaction();
  reader_txn.reset();

  const auto later = Capture(db, *reader, base);
  EXPECT_NE(VerticesOnly(later), first);
  EXPECT_THAT(later.Render(), HasSubstr("p=902"));
  EXPECT_EQ(later.At(base.v3).labels.count("L"), 0u);
  EXPECT_EQ(later.Find(base.v4), nullptr);
  EXPECT_THAT(later.Render(), ::testing::Not(HasSubstr("p=555"))) << "the branch reads main as of the fork";
  ASSERT_TRUE(main_vertex.has_value());
  EXPECT_EQ(later.Find(*main_vertex), nullptr) << "a vertex main created after the fork is not in the branch";
}

// ---- A log recovery must refuse -------------------------------------------------------------------------------------
//
// The branch's WAL is replaced by one assembled from the transactions of a scratch database's WAL. Each case is a
// log the branch's own writers could never have produced; replay must stop at it, the startup must refuse, and
// its files must stay as they are, never applying part of it or guessing.

struct RejectedLog {
  const char *name;
  // Records into a scratch database whose gid counters start at `vertex_gid` / `edge_gid`: one transaction each.
  uint64_t vertex_gid;
  uint64_t edge_gid;
  std::function<void(memgraph::dbms::Database &)> write;
  // The transactions of the scratch WAL the branch's log is made of, in order (repeats allowed).
  std::vector<size_t> txns;
  const char *reason;
};

// A native vertex at gid `gid`, then the same with a label, then the edge between it and a second one.
void WriteTxn(memgraph::dbms::Database &db, const std::function<void(ms::Storage::Accessor &, ms::Storage &)> &body) {
  auto acc = db.Access(ms::WRITE);
  body(*acc, *db.storage());
  Commit(*acc);
}

const std::vector<RejectedLog> &RejectedLogs() {
  static const std::vector<RejectedLog> logs = {
      {"VertexCreatedAtAGidOfTheForkState",
       1,
       0,
       [](memgraph::dbms::Database &db) { WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); }); },
       {0},
       "a gid of the fork state"},
      {"EdgeCreatedAtAGidOfTheForkState",
       100,
       0,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &st) {
           auto a = acc.CreateVertex();
           auto b = acc.CreateVertex();
           ASSERT_TRUE(acc.CreateEdge(&a, &b, st.NameToEdgeType("T")).has_value());
         });
       },
       {0},
       "a gid of the fork state"},
      {"VertexCreatedTwice",
       100,
       0,
       [](memgraph::dbms::Database &db) { WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); }); },
       {0, 0},
       "already exists"},
      {"LabelOfAVertexTheLogNeverCreated",
       100,
       0,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); });
         WriteTxn(db, [](auto &acc, auto &st) {
           auto v = acc.FindVertex(ms::Gid::FromUint(100), ms::View::OLD);
           ASSERT_TRUE(v->AddLabel(st.NameToLabel("N")).has_value());
         });
       },
       {1},
       "not in the branch"},
      {"PropertyOfAVertexTheLogNeverCreated",
       100,
       0,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); });
         WriteTxn(db, [](auto &acc, auto &st) {
           auto v = acc.FindVertex(ms::Gid::FromUint(100), ms::View::OLD);
           ASSERT_TRUE(v->SetProperty(st.NameToProperty("a"), ms::PropertyValue(1)).has_value());
         });
       },
       {1},
       "not in the branch"},
      {"DeleteOfAVertexTheLogNeverCreated",
       100,
       0,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); });
         WriteTxn(db, [](auto &acc, auto &) {
           auto v = acc.FindVertex(ms::Gid::FromUint(100), ms::View::OLD);
           ASSERT_TRUE(acc.DeleteVertex(&*v).has_value());
         });
       },
       {1},
       "not in the branch"},
      {"EdgeOnVerticesTheLogNeverCreated",
       100,
       100,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &) {
           acc.CreateVertex();
           acc.CreateVertex();
         });
         WriteTxn(db, [](auto &acc, auto &st) {
           auto a = acc.FindVertex(ms::Gid::FromUint(100), ms::View::OLD);
           auto b = acc.FindVertex(ms::Gid::FromUint(101), ms::View::OLD);
           ASSERT_TRUE(acc.CreateEdge(&*a, &*b, st.NameToEdgeType("T")).has_value());
         });
       },
       {1},
       "vertex 100"},
      {"LabelAddedWhereTheVertexAlreadyHasIt",
       100,
       0,
       [](memgraph::dbms::Database &db) {
         WriteTxn(db, [](auto &acc, auto &) { acc.CreateVertex(); });
         WriteTxn(db, [](auto &acc, auto &st) {
           auto v = acc.FindVertex(ms::Gid::FromUint(100), ms::View::OLD);
           ASSERT_TRUE(v->AddLabel(st.NameToLabel("N")).has_value());
         });
       },
       {0, 1, 1},
       "already as the WAL states it"},
  };
  return logs;
}

class RejectedLogTest : public VersioningRecoveryEquivalenceTest, public ::testing::WithParamInterface<size_t> {};

TEST_P(RejectedLogTest, ReplayRefusesStartupAndKeepsEveryFile) {
  const auto &log = RejectedLogs()[GetParam()];
  SCOPED_TRACE(log.name);

  // The scratch database: its WAL is the source of the records.
  fs::path scratch_wal;
  {
    memgraph::dbms::Database scratch{
        MakeConfig(/*recover_on_startup=*/false, {.versioning = false}, kOtherStorageDirectory)};
    const memgraph::memory::DbArenaScope arena_scope{&scratch.Arena()};
    Mem(scratch)->vertex_id_.store(log.vertex_gid);
    Mem(scratch)->edge_id_.store(log.edge_gid);
    log.write(scratch);
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
  }
  const auto scratch_files = std::filesystem::directory_iterator{kOtherStorageDirectory / "wal"};
  for (const auto &entry : scratch_files) scratch_wal = entry.path();
  ASSERT_FALSE(scratch_wal.empty());

  uint64_t number = 0;
  Base base{};
  {
    memgraph::dbms::Database db{MakeConfig(/*recover_on_startup=*/false)};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    base = CreateBase(db);
    auto info = db.version_store()->CreateBranch("b");
    ASSERT_TRUE(info.has_value());
    number = info->number;
    auto ctx = Checkout(db, "b");
    ASSERT_NE(ctx, nullptr);
    Env env{db, *ctx, base};
    env.Txn([&](mv::BranchContext &c, auto &) { static_cast<void>(c.CreateNativeVertex()); });
  }
  const auto branch_wals = WalFiles(number);
  ASSERT_EQ(branch_wals.size(), 1u);

  // The branch's file becomes the scratch header followed by the chosen transactions, as a crash image (no summary), so
  // the file is trusted for nothing but its bytes.
  const auto spans = ReadWalSpans(scratch_wal);
  ASSERT_GE(spans.size(), *std::ranges::max_element(log.txns) + 1);
  ASSERT_NO_FATAL_FAILURE(CrashAt(scratch_wal, fs::file_size(scratch_wal)));
  const auto scratch_bytes = ReadFile(scratch_wal);
  auto bytes = scratch_bytes.substr(0, dur::ReadWalHeader(scratch_wal).offset_deltas);
  for (const auto index : log.txns)
    bytes += scratch_bytes.substr(spans[index].begin, spans[index].end - spans[index].begin);
  WriteFile(branch_wals.front(), bytes);

  const auto files = DurableFiles();
  const auto message = StartupRefusal();
  ASSERT_FALSE(message.empty()) << "the log must be refused";
  EXPECT_THAT(message, HasSubstr("Branch 'b' cannot be replayed"));
  EXPECT_THAT(message, HasSubstr(log.reason));
  EXPECT_EQ(DurableFiles(), files);
}

INSTANTIATE_TEST_SUITE_P(Logs, RejectedLogTest, ::testing::Range<size_t>(0, 8),
                         [](const ::testing::TestParamInfo<size_t> &info) {
                           return std::string{RejectedLogs()[info.param].name};
                         });
