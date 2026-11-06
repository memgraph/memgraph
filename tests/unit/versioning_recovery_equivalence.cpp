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

// Recovered branches must read as the live ones did: seeded workloads checked against a model, logs replay must set
// the branch aside for, and snapshot isolation after a restart.
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

// What the branch reads plus all of its bookkeeping: replay must reproduce both exactly.
std::string Visible(const Captured &captured) { return captured.Render(); }

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

// Phases: write, recover and write more, recover again. Each recovered branch must read as the model and as the live
// one did, fork-delete counts included.
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

// Every key-moving writer leaves the same index live and replayed: two-key map update, label-first and property-first
// orders, init, set back to the fork value, part removal, fork label removed then re-added after a key change.
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
    ms::Gid v5;  // {L} p=12 q=13
    {
      auto acc = db.Access(ms::WRITE);
      auto v = acc->CreateVertex();
      ASSERT_TRUE(v.AddLabel(db.storage()->NameToLabel("L")).has_value());
      ASSERT_TRUE(v.SetProperty(db.storage()->NameToProperty("p"), ms::PropertyValue(12)).has_value());
      ASSERT_TRUE(v.SetProperty(db.storage()->NameToProperty("q"), ms::PropertyValue(13)).has_value());
      v5 = v.Gid();
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
    // v5 loses L, then a key property is set while L is off, then L comes back.
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v = env.Fork(v5);
      ASSERT_TRUE(*c.RemoveLabel(v, env.Label("L")));
    });
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v = env.Fork(v5);
      ASSERT_TRUE(c.SetProperty(v, p, ms::PropertyValue(14)).has_value());
    });
    env.Txn([&](mv::BranchContext &c, auto &) {
      auto v = env.Fork(v5);
      ASSERT_TRUE(*c.AddLabel(v, env.Label("L")));
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
                    {base.v4.AsUint(), ints({10, 11})},
                    {v5.AsUint(), ints({14, 13})}}));
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

// A reader opened after the restart keeps its snapshot while a writer commits and main moves on; a later reader sees
// the writer's commits and still reads main as of the fork.
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
    if (i == 1) {
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

// Each case replaces the branch's WAL with transactions of a scratch WAL that the branch's own writers could never
// produce; replay must stop, set the branch aside as unavailable, and leave every file untouched.

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

void WriteTxn(memgraph::dbms::Database &db, const std::function<void(ms::Storage::Accessor &, ms::Storage &)> &body) {
  auto acc = db.Access(ms::WRITE);
  body(*acc, *db.storage());
  Commit(*acc);
}

const std::vector<RejectedLog> &RejectedLogs() {
  static const std::vector<RejectedLog> logs = {
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

TEST_P(RejectedLogTest, ReplaySetsTheBranchAsideAndKeepsEveryFile) {
  const auto &log = RejectedLogs()[GetParam()];
  SCOPED_TRACE(log.name);

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
  const auto unavailable = UnavailableAfterAStart();
  ASSERT_TRUE(unavailable.contains("b")) << "a log that does not replay sets its branch aside";
  EXPECT_THAT(unavailable.at("b"), HasSubstr(log.reason));
  EXPECT_EQ(DurableFiles(), files);
}

INSTANTIATE_TEST_SUITE_P(Logs, RejectedLogTest, ::testing::Range<size_t>(0, 3),
                         [](const ::testing::TestParamInfo<size_t> &info) {
                           return std::string{RejectedLogs()[info.param].name};
                         });
