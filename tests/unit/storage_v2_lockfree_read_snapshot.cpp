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

// Phase 2, batch 1 of the lock-free-read-snapshot suite: concern-A (read visibility).
// A deterministic interleaving harness parks a commit inside the CommitProbe seam so a
// concurrent reader can be opened at a precise instant and its snapshot boundary asserted.

#include <gtest/gtest.h>

#include <optional>
#include <semaphore>
#include <thread>

#include "storage/v2/commit_probe.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

using memgraph::storage::Config;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::View;
using Accessor = memgraph::storage::Storage::Accessor;

namespace {

std::unique_ptr<InMemoryStorage> MakeStorage(bool flag_on) {
  Config config{};
  // Logical visibility tests only: disable GC so nothing reclaims deltas underneath us.
  config.gc.type = Config::Gc::Type::NONE;
  // Isolation left at the default (SNAPSHOT_ISOLATION).
  config.experimental_lockfree_read_snapshot = flag_on;
  return std::make_unique<InMemoryStorage>(config);
}

Gid CreateVertexWithProp(InMemoryStorage &store, int value) {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  auto set = vertex.SetProperty(store.NameToProperty("p"), PropertyValue(value));
  EXPECT_TRUE(set.has_value());
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return gid;
}

// Reads property "p" of vertex `gid` at the accessor's frozen snapshot (View::OLD).
int64_t ReadProp(Accessor &acc, Gid gid) {
  auto vertex = acc.FindVertex(gid, View::OLD);
  EXPECT_TRUE(vertex.has_value());
  auto value = vertex->GetProperty(acc.NameToProperty("p"), View::OLD);
  EXPECT_TRUE(value.has_value());
  return value->ValueInt();
}

}  // namespace

// Reader opened while a commit is parked between mint and publish must see the pre-commit
// value: the in-flight delta still carries the committing transaction's id marker and the
// visibility watermark has not advanced past it.
TEST(LockFreeReadSnapshot, ReaderBeginsBeforePublish_SeesPreCommitValue) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  std::binary_semaphore reached{0};
  std::binary_semaphore resume{0};
  std::optional<bool> commit_ok;

  std::thread committer([&] {
    memgraph::storage::CommitProbe probe;
    probe.before_publish = [&] {
      reached.release();
      resume.acquire();
    };
    store->SetCommitProbe(&probe);

    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    commit_ok = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });

  // Commit is now parked at before_publish: minted, engine_lock released, not yet published.
  reached.acquire();
  {
    auto reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*reader, gid), 1);
  }
  resume.release();
  committer.join();

  ASSERT_TRUE(commit_ok.has_value());
  EXPECT_TRUE(*commit_ok);
  store->SetCommitProbe(nullptr);

  // After publish, a fresh reader observes the committed value.
  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// Reader opened after a full commit sees the new value: the watermark advanced and the
// snapshot boundary is inclusive of a commit whose timestamp equals the reader's start.
TEST(LockFreeReadSnapshot, ReaderBeginsAfterPublish_SeesNewValue) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*reader, gid), 2);
}

// A long-lived reader's snapshot is frozen at open time: a later commit is invisible to it,
// while a reader opened after the commit sees the new value.
TEST(LockFreeReadSnapshot, LongLivedReader_UnaffectedByLaterCommit) {
  auto store = MakeStorage(/*flag_on=*/true);
  const auto gid = CreateVertexWithProp(*store, 1);

  auto long_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto vertex = acc->FindVertex(gid, View::OLD);
    ASSERT_TRUE(vertex.has_value());
    ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  EXPECT_EQ(ReadProp(*long_reader, gid), 1);

  auto fresh_reader = store->Access(memgraph::storage::READ);
  EXPECT_EQ(ReadProp(*fresh_reader, gid), 2);
}

// A transaction's own uncommitted write is visible to itself (View::NEW), independent of the
// snapshot boundary.
TEST(LockFreeReadSnapshot, OwnUncommittedWrite_Visible) {
  auto store = MakeStorage(/*flag_on=*/true);

  auto acc = store->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  const auto gid = vertex.Gid();
  ASSERT_TRUE(vertex.SetProperty(store->NameToProperty("p"), PropertyValue(7)).has_value());

  auto self = acc->FindVertex(gid, View::NEW);
  ASSERT_TRUE(self.has_value());
  auto value = self->GetProperty(store->NameToProperty("p"), View::NEW);
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(value->ValueInt(), 7);
}

// A/B equivalence: the long-lived-reader scenario yields identical observed values whether the
// experimental flag is on or off.
TEST(LockFreeReadSnapshot, OffPath_SameVisibility_AB) {
  for (const bool flag_on : {true, false}) {
    auto store = MakeStorage(flag_on);
    const auto gid = CreateVertexWithProp(*store, 1);

    auto long_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*long_reader, gid), 1) << "flag_on=" << flag_on;

    {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto vertex = acc->FindVertex(gid, View::OLD);
      ASSERT_TRUE(vertex.has_value());
      ASSERT_TRUE(vertex->SetProperty(store->NameToProperty("p"), PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    EXPECT_EQ(ReadProp(*long_reader, gid), 1) << "flag_on=" << flag_on;

    auto fresh_reader = store->Access(memgraph::storage::READ);
    EXPECT_EQ(ReadProp(*fresh_reader, gid), 2) << "flag_on=" << flag_on;
  }
}
