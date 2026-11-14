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

// A stub is a stock object, so the engine's own MVCC versions, conflicts, aborts and collects it. These tests pin what
// a branch relies on: first-writer-wins, snapshot/read-committed visibility, atomic commits, safe collection.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "tests/unit/versioning_stub_codec.hpp"
#include "utils/on_scope_exit.hpp"
#include "versioning/stub_state.hpp"

namespace ms = memgraph::storage;
namespace stub = memgraph::versioning::stub;
using memgraph::versioning::StubState;

namespace {

ms::LabelId L(uint32_t id) { return ms::LabelId::FromUint(id); }

ms::PropertyId P(uint32_t id) { return ms::PropertyId::FromUint(id); }

ms::PropertyValue V(int64_t v) { return ms::PropertyValue{v}; }

constexpr auto kSnapshot = ms::IsolationLevel::SNAPSHOT_ISOLATION;
constexpr auto kReadCommitted = ms::IsolationLevel::READ_COMMITTED;

class StubMvccTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage = std::make_unique<ms::InMemoryStorage>(config);
  }

  std::unique_ptr<ms::Storage::Accessor> Writer(ms::IsolationLevel level = kSnapshot) {
    return storage->Access(ms::WRITE, level, std::nullopt);
  }

  std::unique_ptr<ms::Storage::Accessor> Reader(ms::IsolationLevel level = kSnapshot) {
    return storage->Access(ms::READ, level, std::nullopt);
  }

  static void Commit(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid CreateStub() {
    auto acc = Writer();
    auto v = acc->CreateVertex();
    auto const gid = v.Gid();
    Commit(*acc);
    return gid;
  }

  static StubState Read(ms::Storage::Accessor &acc, ms::Gid gid, ms::View view = ms::View::NEW) {
    auto v = acc.FindVertex(gid, view);
    EXPECT_TRUE(v.has_value());
    auto state = stub::Decode(*v, view);
    EXPECT_TRUE(state.has_value());
    return std::move(*state);
  }

  // Returns the error of the first failed write, if any.
  template <typename Edit>
  static std::optional<ms::Error> Write(ms::Storage::Accessor &acc, ms::Gid gid, Edit &&edit) {
    auto v = acc.FindVertex(gid, ms::View::NEW);
    EXPECT_TRUE(v.has_value());
    auto before = stub::Decode(*v, ms::View::NEW);
    EXPECT_TRUE(before.has_value());
    auto after = before->Copy();
    edit(after);
    return stub::Apply(*v, *before, after);
  }

  std::unique_ptr<ms::InMemoryStorage> storage;
};

}  // namespace

TEST_F(StubMvccTest, TheFirstWriterOfAStubWinsAndTheSecondMustAbort) {
  for (bool first_commits_first : {true, false}) {
    SCOPED_TRACE(first_commits_first);
    auto const gid = CreateStub();
    auto a = Writer();
    auto b = Writer();

    EXPECT_FALSE(Write(*a, gid, [](StubState &s) { s.SetProperty(P(1), V(1)); }));
    auto const err = Write(*b, gid, [](StubState &s) { s.SetProperty(P(2), V(2)); });
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(*err, ms::Error::SERIALIZATION_ERROR);

    if (first_commits_first) Commit(*a);
    b->Abort();
    if (!first_commits_first) Commit(*a);

    auto check = Reader();
    auto const state = Read(*check, gid);
    EXPECT_EQ(state.properties.Properties().size(), 1U);
    EXPECT_EQ(state.properties.Properties().at(P(1)), V(1));
    check->Abort();
  }
}

TEST_F(StubMvccTest, ALaterWriterConflictsWithACommitItsSnapshotDoesNotSee) {
  auto const gid = CreateStub();
  auto late = Writer();  // its snapshot predates the commit below
  {
    auto early = Writer();
    EXPECT_FALSE(Write(*early, gid, [](StubState &s) { s.AddLabel(L(1)); }));
    Commit(*early);
  }
  auto const err = Write(*late, gid, [](StubState &s) { s.AddLabel(L(2)); });
  ASSERT_TRUE(err.has_value());
  EXPECT_EQ(*err, ms::Error::SERIALIZATION_ERROR);
  late->Abort();
}

TEST_F(StubMvccTest, WritersOfDifferentStubsDoNotConflict) {
  auto const g1 = CreateStub();
  auto const g2 = CreateStub();
  auto a = Writer();
  auto b = Writer();
  EXPECT_FALSE(Write(*a, g1, [](StubState &s) { s.SetProperty(P(1), V(1)); }));
  EXPECT_FALSE(Write(*b, g2, [](StubState &s) { s.SetProperty(P(1), V(2)); }));
  Commit(*a);
  Commit(*b);

  auto r = Reader();
  EXPECT_EQ(Read(*r, g1, ms::View::OLD).properties.Properties().at(P(1)), V(1));
  EXPECT_EQ(Read(*r, g2, ms::View::OLD).properties.Properties().at(P(1)), V(2));
  r->Abort();
}

TEST_F(StubMvccTest, ASnapshotReaderKeepsTheOldStateAndAReadCommittedOneSeesTheNew) {
  auto const gid = CreateStub();
  auto snapshot = Reader(kSnapshot);
  auto read_committed = Reader(kReadCommitted);
  auto const before_snapshot = Read(*snapshot, gid, ms::View::OLD);
  EXPECT_TRUE(before_snapshot.properties.Properties().empty());

  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) {
      s.SetProperty(P(1), V(1));
      s.RemoveLabel(L(7));
      s.RemoveProperty(P(9));
    }));
    Commit(*w);
  }

  auto const old_state = Read(*snapshot, gid, ms::View::OLD);
  EXPECT_TRUE(old_state.properties.Properties().empty());
  EXPECT_TRUE(old_state.removed_labels.empty());
  EXPECT_TRUE(old_state.removed_pids.empty());

  auto const new_state = Read(*read_committed, gid, ms::View::OLD);
  EXPECT_EQ(new_state.properties.Properties().at(P(1)), V(1));
  EXPECT_EQ(new_state.removed_labels, std::vector{L(7)});
  EXPECT_EQ(new_state.removed_pids, std::vector{P(9)});
  snapshot->Abort();
  read_committed->Abort();
}

TEST_F(StubMvccTest, AnAbortRestoresTheStub) {
  auto const gid = CreateStub();
  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) {
      s.SetProperty(P(1), V(1));
      s.AddLabel(L(3));
    }));
    Commit(*w);
  }
  auto const committed = [&] {
    auto r = Reader();
    auto state = Read(*r, gid);
    r->Abort();
    return state;
  }();

  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) {
      s.SetProperty(P(1), V(2));
      s.RemoveProperty(P(1));
      s.RemoveLabel(L(4));
      s.DropAddedLabel(L(3));
    }));
    auto const inside = Read(*w, gid);
    EXPECT_NE(inside.removed_pids, committed.removed_pids);
    w->Abort();
  }
  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) { s.MarkDeleted(); }));
    EXPECT_TRUE(Read(*w, gid).deleted);
    EXPECT_TRUE(*stub::IsDeleted(*w->FindVertex(gid, ms::View::NEW), ms::View::NEW));
    w->Abort();
  }

  auto r = Reader();
  EXPECT_FALSE(*stub::IsDeleted(*r->FindVertex(gid, ms::View::NEW), ms::View::NEW));
  auto const state = Read(*r, gid);
  EXPECT_FALSE(state.deleted);
  EXPECT_EQ(state.added_labels, committed.added_labels);
  EXPECT_EQ(state.removed_labels, committed.removed_labels);
  EXPECT_EQ(state.removed_pids, committed.removed_pids);
  EXPECT_EQ(state.properties.Properties(), committed.properties.Properties());
  r->Abort();
}

TEST_F(StubMvccTest, ACommitOfSeveralStubsIsAllOrNothingForConcurrentSnapshots) {
  auto const g1 = CreateStub();
  auto const g2 = CreateStub();
  auto const g3 = CreateStub();
  auto before = Reader();
  auto w = Writer();
  for (auto gid : {g1, g2, g3}) {
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) { s.SetProperty(P(1), V(5)); }));
  }
  auto during = Reader();
  Commit(*w);
  auto after = Reader();

  for (auto gid : {g1, g2, g3}) {
    EXPECT_TRUE(Read(*before, gid, ms::View::OLD).properties.Properties().empty());
    EXPECT_TRUE(Read(*during, gid, ms::View::OLD).properties.Properties().empty());
    EXPECT_EQ(Read(*after, gid, ms::View::OLD).properties.Properties().at(P(1)), V(5));
  }
  before->Abort();
  during->Abort();
  after->Abort();
}

TEST_F(StubMvccTest, CollectionKeepsTheStateAPinnedReaderStillNeeds) {
  auto const gid = CreateStub();
  auto pinned = Reader();
  EXPECT_TRUE(Read(*pinned, gid, ms::View::OLD).properties.Properties().empty());

  for (int64_t i = 1; i <= 5; ++i) {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [i](StubState &s) { s.SetProperty(P(1), V(i)); }));
    Commit(*w);
    storage->FreeMemory({}, false);
  }
  EXPECT_TRUE(Read(*pinned, gid, ms::View::OLD).properties.Properties().empty())
      << "a collection cycle reclaimed what a pinned snapshot still reads";
  pinned->Abort();

  storage->FreeMemory({}, false);
  storage->FreeMemory({}, false);
  auto r = Reader();
  EXPECT_EQ(Read(*r, gid, ms::View::OLD).properties.Properties().at(P(1)), V(5));
  r->Abort();

  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) { s.MarkDeleted(); }));
    Commit(*w);
  }
  storage->FreeMemory({}, false);
  storage->FreeMemory({}, false);
  auto after_delete = Reader();
  EXPECT_TRUE(Read(*after_delete, gid).deleted) << "a collected delete marker must still read as deleted";
  after_delete->Abort();
}

// Each writer keeps P(1) == P(2); a snapshot reader seeing them differ saw a torn commit or collected version.
// Read-committed readers are only exercised, not asserted: a commit landing mid-read can tear a multi-key read.
TEST_F(StubMvccTest, WritersReadersAndCollectionNeverShowATornStub) {
  constexpr int kStubs = 4;
  constexpr int kWriters = 4;
  constexpr int kReaders = 3;
  constexpr int kWritesPerWriter = 150;
  std::vector<ms::Gid> gids;
  for (int i = 0; i < kStubs; ++i) gids.push_back(CreateStub());

  std::atomic<bool> stop{false};
  std::atomic<int> torn{0};
  std::atomic<int> committed{0};

  std::vector<std::thread> readers;
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&, r] {
      while (!stop.load(std::memory_order_acquire)) {
        auto acc = Reader(r % 2 == 0 ? kSnapshot : kReadCommitted);
        for (auto gid : gids) {
          auto v = acc->FindVertex(gid, ms::View::OLD);
          if (!v) continue;
          auto state = stub::Decode(*v, ms::View::OLD);
          if (!state) continue;
          auto const props = state->properties.Properties();
          auto const p1 = props.find(P(1));
          auto const p2 = props.find(P(2));
          bool const torn_read =
              (p1 == props.end()) != (p2 == props.end()) || (p1 != props.end() && p1->second != p2->second);
          if (torn_read && r % 2 == 0) torn.fetch_add(1);
        }
        acc->Abort();
      }
    });
  }
  std::thread collector{[&] {
    while (!stop.load(std::memory_order_acquire)) {
      storage->FreeMemory({}, false);
      std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
  }};

  std::vector<std::thread> writers;
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      std::mt19937 rng{static_cast<uint32_t>(w)};
      for (int i = 0; i < kWritesPerWriter;) {
        auto const gid = gids[rng() % kStubs];
        auto acc = Writer();
        auto const value = (static_cast<int64_t>(w) * 1000000) + i;
        auto const err = Write(*acc, gid, [value](StubState &s) {
          s.SetProperty(P(1), V(value));
          s.SetProperty(P(2), V(value));
        });
        if (err) {
          acc->Abort();
          continue;
        }
        if (acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
          committed.fetch_add(1);
          ++i;
        }
      }
    });
  }
  for (auto &t : writers) t.join();
  stop.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  collector.join();

  EXPECT_EQ(torn.load(), 0);
  EXPECT_EQ(committed.load(), kWriters * kWritesPerWriter);
  auto r = Reader();
  for (auto gid : gids) {
    auto const props = Read(*r, gid, ms::View::OLD).properties.Properties();
    ASSERT_EQ(props.size(), 2U);
    EXPECT_EQ(props.at(P(1)), props.at(P(2)));
  }
  r->Abort();
}

// A deleted fork edge is an entry of type Mark(T) in both endpoint stubs' adjacency, written with stock vertex deltas.

namespace {

class EdgeMarkerTest : public StubMvccTest {
 protected:
  void SetUp() override {
    StubMvccTest::SetUp();
    type = storage->NameToEdgeType("T");
    edge_stub = std::make_unique<ms::Edge>(ms::Gid::FromUint(5000), nullptr);
    u = CreateStub();
    v = CreateStub();
  }

  bool Link(ms::Storage::Accessor &acc, ms::EdgeDirection direction, ms::Gid stub, ms::Gid other) {
    auto s = acc.FindVertex(stub, ms::View::NEW);
    auto o = acc.FindVertex(other, ms::View::NEW);
    EXPECT_TRUE(s.has_value() && o.has_value());
    return stub::LinkEdgeMarker(
        acc.GetTransaction(), *storage, s->vertex_, direction, type, o->vertex_, edge_stub.get());
  }

  bool LinkBoth(ms::Storage::Accessor &acc) {
    return Link(acc, ms::EdgeDirection::OUT, u, v) && Link(acc, ms::EdgeDirection::IN, v, u);
  }

  size_t Entries(ms::Storage::Accessor &acc, ms::Gid stub, ms::EdgeDirection direction, ms::View view,
                 bool *is_edge_stub = nullptr) {
    auto vertex = acc.FindVertex(stub, view);
    EXPECT_TRUE(vertex.has_value());
    auto expanded = direction == ms::EdgeDirection::OUT ? vertex->OutEdges(view) : vertex->InEdges(view);
    EXPECT_TRUE(expanded.has_value());
    size_t count = 0;
    for (auto const &edge : expanded->edges) {
      if (edge.EdgeType() != stub::Mark(type)) continue;
      ++count;
      if (is_edge_stub != nullptr) *is_edge_stub = edge.edge_.ptr == edge_stub.get();
    }
    return count;
  }

  ms::EdgeTypeId type;
  std::unique_ptr<ms::Edge> edge_stub;
  ms::Gid u, v;
};

}  // namespace

TEST_F(EdgeMarkerTest, AMarkerEntryIsVisibleByViewAsAnEdgeCreationIs) {
  auto writer = Writer();
  auto snapshot = Reader(kSnapshot);
  auto read_committed = Reader(kReadCommitted);
  ASSERT_TRUE(LinkBoth(*writer));

  bool is_edge_stub = false;
  EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::NEW, &is_edge_stub), 1U);
  EXPECT_TRUE(is_edge_stub) << "the entry holds the edge stub";
  EXPECT_EQ(Entries(*writer, v, ms::EdgeDirection::IN, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::OLD), 0U) << "not before the command advances";
  writer->AdvanceCommand();
  EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::OLD), 1U);
  EXPECT_EQ(Entries(*snapshot, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
  EXPECT_EQ(Entries(*read_committed, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
  EXPECT_EQ(Entries(*read_committed, v, ms::EdgeDirection::IN, ms::View::NEW), 0U);

  Commit(*writer);
  EXPECT_EQ(Entries(*snapshot, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U) << "a snapshot older than the commit";
  EXPECT_EQ(Entries(*read_committed, u, ms::EdgeDirection::OUT, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*read_committed, v, ms::EdgeDirection::IN, ms::View::NEW), 1U);
  auto later = Reader();
  EXPECT_EQ(Entries(*later, u, ms::EdgeDirection::OUT, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*later, v, ms::EdgeDirection::IN, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*later, u, ms::EdgeDirection::IN, ms::View::NEW), 0U);
}

// LinkEdgeMarker must invalidate the delta-chain cache as a stock edge creation does. Filling it takes a walk over
// >= threshold deltas, which the writer only meets in another transaction's in-flight ones, hence create-then-abort.
TEST_F(EdgeMarkerTest, AMarkerIsVisibleToTheWritersOwnCachedExpansion) {
  auto const saved_threshold = FLAGS_delta_chain_cache_threshold;
  FLAGS_delta_chain_cache_threshold = 2;
  auto const restore_threshold =
      memgraph::utils::OnScopeExit{[&] { FLAGS_delta_chain_cache_threshold = saved_threshold; }};

  auto writer = Writer();
  writer->GetTransaction()->EnableDeltaChainCache();
  ASSERT_TRUE(writer->GetTransaction()->UseCache());
  auto const cached = [&](ms::EdgeDirection direction, ms::Gid stub) {
    auto vertex = writer->FindVertex(stub, ms::View::NEW);
    auto const &cache = writer->GetTransaction()->manyDeltasCache;
    auto const no_types = std::vector<ms::EdgeTypeId>{};
    return (direction == ms::EdgeDirection::OUT ? cache.GetOutEdges(ms::View::NEW, vertex->vertex_, nullptr, no_types)
                                                : cache.GetInEdges(ms::View::NEW, vertex->vertex_, nullptr, no_types))
        .has_value();
  };

  {
    auto other = Writer();
    auto from = other->FindVertex(u, ms::View::NEW);
    auto to = other->FindVertex(v, ms::View::NEW);
    ASSERT_TRUE(from.has_value() && to.has_value());
    for (uint64_t i = 0; i < 4; ++i) {
      ASSERT_TRUE(static_cast<ms::ReplicationAccessor &>(*other)
                      .CreateEdgeEx(&*from, &*to, type, ms::Gid::FromUint(9100 + i))
                      .has_value());
    }
    EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
    EXPECT_EQ(Entries(*writer, v, ms::EdgeDirection::IN, ms::View::NEW), 0U);
    other->Abort();
  }
  ASSERT_TRUE(cached(ms::EdgeDirection::OUT, u))
      << "the expansions must have filled the cache, or this test is vacuous";
  ASSERT_TRUE(cached(ms::EdgeDirection::IN, v));

  ASSERT_TRUE(LinkBoth(*writer));
  EXPECT_FALSE(cached(ms::EdgeDirection::OUT, u)) << "the out-entry must drop the stale out-expansion";
  EXPECT_FALSE(cached(ms::EdgeDirection::IN, v)) << "the in-entry must drop the stale in-expansion";
  EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*writer, v, ms::EdgeDirection::IN, ms::View::NEW), 1U);
  writer->Abort();
}

TEST_F(EdgeMarkerTest, AnAbortRemovesTheEntriesAndLeavesTheEdgeCountAsItWas) {
  auto const before = storage->edge_count_.load();
  {
    auto writer = Writer();
    ASSERT_TRUE(LinkBoth(*writer));
    // Two out-entries and two in-entries: the abort must add back exactly one per out-entry, none per in-entry.
    ASSERT_TRUE(Link(*writer, ms::EdgeDirection::OUT, v, u));
    ASSERT_TRUE(Link(*writer, ms::EdgeDirection::IN, u, v));
    EXPECT_EQ(storage->edge_count_.load(), before) << "a marker is no edge";
    writer->Abort();
  }
  EXPECT_EQ(storage->edge_count_.load(), before) << "the abort undoes each out-entry as it undoes an edge creation";
  auto reader = Reader(kReadCommitted);
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
  EXPECT_EQ(Entries(*reader, v, ms::EdgeDirection::IN, ms::View::NEW), 0U);

  auto committed = Writer();
  ASSERT_TRUE(LinkBoth(*committed));
  Commit(*committed);
  EXPECT_EQ(storage->edge_count_.load(), before);
}

TEST_F(EdgeMarkerTest, ASecondMarkerOnOneStubConflicts) {
  auto first = Writer();
  auto second = Writer();
  ASSERT_TRUE(Link(*first, ms::EdgeDirection::OUT, u, v));
  EXPECT_FALSE(Link(*second, ms::EdgeDirection::OUT, u, v));
  EXPECT_TRUE(second->TransactionHasSerializationError());
  second->Abort();
  first->Abort();
}

TEST_F(EdgeMarkerTest, AnEdgeCreatedOnAStubConflictsWithAnInFlightMarkerInEitherOrder) {
  auto const create = [&](ms::Storage::Accessor &acc) {
    auto from = acc.FindVertex(u, ms::View::NEW);
    auto to = acc.FindVertex(v, ms::View::NEW);
    EXPECT_TRUE(from.has_value() && to.has_value());
    return static_cast<ms::ReplicationAccessor &>(acc).CreateEdgeEx(&*from, &*to, type, ms::Gid::FromUint(9000));
  };
  {
    auto marker = Writer();
    auto creator = Writer();
    ASSERT_TRUE(Link(*marker, ms::EdgeDirection::OUT, u, v));
    auto created = create(*creator);
    ASSERT_FALSE(created.has_value());
    EXPECT_EQ(created.error(), ms::Error::SERIALIZATION_ERROR);
    creator->Abort();
    marker->Abort();
  }
  {
    auto marker = Writer();
    auto creator = Writer();
    ASSERT_TRUE(create(*creator).has_value());
    EXPECT_FALSE(Link(*marker, ms::EdgeDirection::OUT, u, v));
    EXPECT_TRUE(marker->TransactionHasSerializationError());
    marker->Abort();
    creator->Abort();
  }
}

TEST_F(EdgeMarkerTest, AWriterKeepsWritingTheStubAfterItsOwnMarker) {
  auto writer = Writer();
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::OUT, u, v));
  EXPECT_FALSE(Write(*writer, u, [](StubState &s) { s.SetProperty(P(1), V(1)); }));
  auto from = writer->FindVertex(u, ms::View::NEW);
  auto to = writer->FindVertex(v, ms::View::NEW);
  ASSERT_TRUE(from.has_value() && to.has_value());
  EXPECT_TRUE(static_cast<ms::ReplicationAccessor &>(*writer)
                  .CreateEdgeEx(&*from, &*to, type, ms::Gid::FromUint(9001))
                  .has_value());
  EXPECT_TRUE(Link(*writer, ms::EdgeDirection::OUT, u, v)) << "and a second marker";
  Commit(*writer);
}

TEST_F(EdgeMarkerTest, ASelfLoopIsOneEntryPerList) {
  auto writer = Writer();
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::OUT, u, u));
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::IN, u, u));
  Commit(*writer);
  auto reader = Reader();
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::OUT, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::IN, ms::View::NEW), 1U);
}
