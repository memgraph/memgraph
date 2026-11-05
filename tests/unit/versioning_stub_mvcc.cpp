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

// A stub is a stock object, so the diff engine's own MVCC versions, conflicts, aborts and collects it. These tests pin
// the behaviour a branch relies on: first-writer-wins per stub, snapshot and read-committed visibility, all-or-nothing
// commits across stubs, and collection that keeps what a pinned reader still needs.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "versioning/stub_merge.hpp"
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

  // The stub's committed-or-own state as the accessor sees it now.
  static StubState Read(ms::Storage::Accessor &acc, ms::Gid gid, ms::View view = ms::View::NEW) {
    auto v = acc.FindVertex(gid, view);
    EXPECT_TRUE(v.has_value());
    auto state = stub::Decode(*v, view);
    EXPECT_TRUE(state.has_value());
    return std::move(*state);
  }

  // Applies `edit` to the stub through the codec, as a branch write does. Returns the error of the first failed write.
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

  auto r = Reader();
  auto const state = Read(*r, gid);
  EXPECT_EQ(state.added_labels, committed.added_labels);
  EXPECT_EQ(state.removed_labels, committed.removed_labels);
  EXPECT_EQ(state.removed_pids, committed.removed_pids);
  EXPECT_EQ(state.properties.Properties(), committed.properties.Properties());
  r->Abort();
}

TEST_F(StubMvccTest, AnAbortedDeleteLeavesTheStubUndeleted) {
  auto const gid = CreateStub();
  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) { s.MarkDeleted(); }));
    EXPECT_TRUE(Read(*w, gid).deleted);
    w->Abort();
  }
  auto r = Reader();
  EXPECT_FALSE(Read(*r, gid).deleted);
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
}

TEST_F(StubMvccTest, ACollectedDeleteMarkerStillReadsAsDeleted) {
  auto const gid = CreateStub();
  {
    auto w = Writer();
    EXPECT_FALSE(Write(*w, gid, [](StubState &s) { s.MarkDeleted(); }));
    Commit(*w);
  }
  storage->FreeMemory({}, false);
  storage->FreeMemory({}, false);
  auto r = Reader();
  EXPECT_TRUE(Read(*r, gid).deleted);
  r->Abort();
}

// One transaction keeps two properties of a stub equal, so a snapshot reader that ever sees them differ saw a torn
// commit or a collected version. Writers retry on conflict; a collector runs throughout. A read-committed reader is
// only exercised: it decides per delta whether the writer has committed, so a commit landing mid-read can tear a
// multi-key read of one object, for any stock vertex.
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

///// MERGED READS UNDER READ_COMMITTED
//
// A read-committed reader whose delta walk straddles a commit sees a prefix of that commit's writes, so a stub can read
// as holding a value and its removal marker at once. stub_merge.hpp prefers the value, and ApplyProperties orders its
// writes so that every prefix is the old or the new state.

namespace {

// A committed vertex standing in for main@fork: P(1) = 100.
ms::Gid CreateFork(ms::Storage &storage) {
  auto acc = storage.Access(ms::WRITE, kSnapshot, std::nullopt);
  auto v = acc->CreateVertex();
  EXPECT_TRUE(v.SetProperty(P(1), V(100)).has_value());
  auto const gid = v.Gid();
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return gid;
}

}  // namespace

TEST_F(StubMvccTest, AValueAndItsRemovalMarkerTogetherReadAsTheValue) {
  auto const fork_gid = CreateFork(*storage);
  auto const gid = CreateStub();
  {
    auto acc = Writer();
    auto v = acc->FindVertex(gid, ms::View::NEW);
    ASSERT_TRUE(v->SetProperty(P(1), V(5)).has_value());
    ASSERT_TRUE(v->SetProperty(stub::Mark(P(1)), ms::PropertyValue{true}).has_value());
    Commit(*acc);
  }
  auto r = Reader(kReadCommitted);
  auto const stub_v = r->FindVertex(gid, ms::View::NEW);
  auto const fork = r->FindVertex(fork_gid, ms::View::NEW);
  EXPECT_EQ(*stub::MergeGetProperty(*stub_v, *fork, P(1), ms::View::NEW), V(5));
  EXPECT_GT(*stub::MergeGetPropertySize(*stub_v, *fork, P(1), ms::View::NEW), 0U);
  auto const merged = stub::MergeProperties(*stub_v->Properties(ms::View::NEW), *fork);
  ASSERT_TRUE(merged.has_value());
  EXPECT_EQ(merged->at(P(1)), V(5));

  // The marker alone is a removal.
  auto const only_marker = CreateStub();
  {
    auto acc = Writer();
    auto v = acc->FindVertex(only_marker, ms::View::NEW);
    ASSERT_TRUE(v->SetProperty(stub::Mark(P(1)), ms::PropertyValue{true}).has_value());
    Commit(*acc);
  }
  auto const removed_v = r->FindVertex(only_marker, ms::View::NEW);
  EXPECT_TRUE(stub::MergeGetProperty(*removed_v, *fork, P(1), ms::View::NEW)->IsNull());
  EXPECT_EQ(*stub::MergeGetPropertySize(*removed_v, *fork, P(1), ms::View::NEW), 0U);
  EXPECT_FALSE(stub::MergeProperties(*removed_v->Properties(ms::View::NEW), *fork)->contains(P(1)));
  r->Abort();
}

// Newest delta first: the order a delta walk undoes the writes in, so the order a torn read keeps them out.
TEST_F(StubMvccTest, ApplyTurnsAMarkerOnBeforeDroppingTheValueAndOffAfterSettingIt) {
  auto const key_of = [](const ms::Delta *delta) {
    EXPECT_EQ(delta->action, ms::Delta::Action::SET_PROPERTY);
    return delta->property.key;
  };
  auto const gid = CreateStub();
  {
    auto seed = Writer();
    ASSERT_FALSE(Write(*seed, gid, [](StubState &s) { s.SetProperty(P(1), V(5)); }));
    Commit(*seed);
  }
  {  // set -> removed: the marker goes on first, so the value is the newest write
    auto acc = Writer();
    ASSERT_FALSE(Write(*acc, gid, [](StubState &s) { s.RemoveProperty(P(1)); }));
    auto const *head = acc->FindVertex(gid, ms::View::NEW)->vertex_->delta();
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(key_of(head), P(1));
    EXPECT_EQ(key_of(head->next.load()), stub::Mark(P(1)));
    Commit(*acc);
  }
  {  // removed -> set: the marker comes off last, so it is the newest write
    auto acc = Writer();
    ASSERT_FALSE(Write(*acc, gid, [](StubState &s) { s.SetProperty(P(1), V(6)); }));
    auto const *head = acc->FindVertex(gid, ms::View::NEW)->vertex_->delta();
    ASSERT_NE(head, nullptr);
    EXPECT_EQ(key_of(head), stub::Mark(P(1)));
    EXPECT_EQ(key_of(head->next.load()), P(1));
    Commit(*acc);
  }
}

TEST_F(StubMvccTest, AReadCommittedMergedReadOfAFlippedPropertyIsAlwaysTheOldOrTheNewValue) {
  constexpr int kFlips = 3000;
  auto const fork_gid = CreateFork(*storage);
  auto const gid = CreateStub();
  auto const five_size = [] {
    ms::PropertyStore store;
    store.SetProperty(P(1), V(5));
    return uint64_t{store.PropertySize(P(1))};
  }();

  {
    auto seed = Writer();
    ASSERT_FALSE(Write(*seed, gid, [](StubState &s) { s.SetProperty(P(1), V(5)); }));
    Commit(*seed);
  }

  std::atomic<bool> stop{false};
  std::atomic<int> wrong{0};
  std::atomic<int> reads{0};
  std::vector<std::thread> readers;
  for (int r = 0; r < 3; ++r) {
    readers.emplace_back([&] {
      while (!stop.load(std::memory_order_acquire)) {
        auto acc = Reader(kReadCommitted);
        auto const stub_v = acc->FindVertex(gid, ms::View::NEW);
        auto const fork = acc->FindVertex(fork_gid, ms::View::NEW);
        auto const value = stub::MergeGetProperty(*stub_v, *fork, P(1), ms::View::NEW);
        auto const size = stub::MergeGetPropertySize(*stub_v, *fork, P(1), ms::View::NEW);
        auto const all = stub::MergeProperties(*stub_v->Properties(ms::View::NEW), *fork);
        // Never the fork's 100: the property is either the branch's 5 or removed.
        if (!value || (!value->IsNull() && *value != V(5))) wrong.fetch_add(1);
        if (!size || (*size != 0 && *size != five_size)) wrong.fetch_add(1);
        if (!all || (all->contains(P(1)) && all->at(P(1)) != V(5))) wrong.fetch_add(1);
        reads.fetch_add(1);
        acc->Abort();
      }
    });
  }
  bool removed = false;
  for (int i = 0; i < kFlips; ++i) {
    auto acc = Writer();
    ASSERT_FALSE(Write(*acc, gid, [&](StubState &s) {
      if (removed) {
        s.SetProperty(P(1), V(5));
      } else {
        s.RemoveProperty(P(1));
      }
    }));
    Commit(*acc);
    removed = !removed;
  }
  stop.store(true, std::memory_order_release);
  for (auto &t : readers) t.join();
  EXPECT_EQ(wrong.load(), 0);
  EXPECT_GT(reads.load(), 0);
}
