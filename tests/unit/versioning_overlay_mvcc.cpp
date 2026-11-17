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

// LinkEdgeMarker (overlay_state.hpp): visibility, conflicts and edge_count_ balance of the stock-created marker
// entries.

#include <cstdint>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/vertex_info_cache.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/on_scope_exit.hpp"
#include "versioning/overlay_state.hpp"

namespace ms = memgraph::storage;
namespace overlay = memgraph::versioning::overlay;

namespace {

constexpr auto kSnapshot = ms::IsolationLevel::SNAPSHOT_ISOLATION;
constexpr auto kReadCommitted = ms::IsolationLevel::READ_COMMITTED;

class LinkEdgeMarkerTest : public testing::Test {
 protected:
  void SetUp() override {
    ms::Config config;
    config.gc = {.type = ms::Config::Gc::Type::NONE};
    storage = std::make_unique<ms::InMemoryStorage>(config);
    type = storage->NameToEdgeType("T");
    edge_overlay = std::make_unique<ms::Edge>(ms::Gid::FromUint(5000), nullptr);
    u = CreateOverlay();
    v = CreateOverlay();
  }

  std::unique_ptr<ms::Storage::Accessor> Writer() { return storage->Access(ms::WRITE, kSnapshot, std::nullopt); }

  std::unique_ptr<ms::Storage::Accessor> Reader(ms::IsolationLevel level = kSnapshot) {
    return storage->Access(ms::READ, level, std::nullopt);
  }

  static void Commit(ms::Storage::Accessor &acc) {
    ASSERT_TRUE(acc.PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  ms::Gid CreateOverlay() {
    auto acc = Writer();
    auto vertex = acc->CreateVertex();
    auto const gid = vertex.Gid();
    Commit(*acc);
    return gid;
  }

  bool Link(ms::Storage::Accessor &acc, ms::EdgeDirection direction, ms::Gid overlay, ms::Gid other) {
    auto s = acc.FindVertex(overlay, ms::View::NEW);
    auto o = acc.FindVertex(other, ms::View::NEW);
    EXPECT_TRUE(s.has_value() && o.has_value());
    return overlay::LinkEdgeMarker(
        acc.GetTransaction(), *storage, s->vertex_, direction, type, o->vertex_, edge_overlay.get());
  }

  bool LinkBoth(ms::Storage::Accessor &acc) {
    return Link(acc, ms::EdgeDirection::OUT, u, v) && Link(acc, ms::EdgeDirection::IN, v, u);
  }

  size_t Entries(ms::Storage::Accessor &acc, ms::Gid overlay, ms::EdgeDirection direction, ms::View view,
                 bool *is_edge_overlay = nullptr) {
    auto vertex = acc.FindVertex(overlay, view);
    EXPECT_TRUE(vertex.has_value());
    auto expanded = direction == ms::EdgeDirection::OUT ? vertex->OutEdges(view) : vertex->InEdges(view);
    EXPECT_TRUE(expanded.has_value());
    size_t count = 0;
    for (auto const &edge : expanded->edges) {
      if (edge.EdgeType() != overlay::Mark(type)) continue;
      ++count;
      if (is_edge_overlay != nullptr) *is_edge_overlay = edge.edge_.ptr == edge_overlay.get();
    }
    return count;
  }

  std::unique_ptr<ms::InMemoryStorage> storage;
  ms::EdgeTypeId type;
  std::unique_ptr<ms::Edge> edge_overlay;
  ms::Gid u, v;
};

}  // namespace

TEST_F(LinkEdgeMarkerTest, AMarkerEntryIsVisibleByViewAsAnEdgeCreationIs) {
  auto writer = Writer();
  auto snapshot = Reader(kSnapshot);
  auto read_committed = Reader(kReadCommitted);
  ASSERT_TRUE(LinkBoth(*writer));

  bool is_edge_overlay = false;
  EXPECT_EQ(Entries(*writer, u, ms::EdgeDirection::OUT, ms::View::NEW, &is_edge_overlay), 1U);
  EXPECT_TRUE(is_edge_overlay) << "the entry holds the edge overlay";
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
TEST_F(LinkEdgeMarkerTest, AMarkerIsVisibleToTheWritersOwnCachedExpansion) {
  auto const saved_threshold = FLAGS_delta_chain_cache_threshold;
  FLAGS_delta_chain_cache_threshold = 2;
  auto const restore_threshold =
      memgraph::utils::OnScopeExit{[&] { FLAGS_delta_chain_cache_threshold = saved_threshold; }};

  auto writer = Writer();
  writer->GetTransaction()->EnableDeltaChainCache();
  ASSERT_TRUE(writer->GetTransaction()->UseCache());
  auto const cached = [&](ms::EdgeDirection direction, ms::Gid overlay) {
    auto vertex = writer->FindVertex(overlay, ms::View::NEW);
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

TEST_F(LinkEdgeMarkerTest, AnAbortRemovesTheEntriesAndLeavesTheEdgeCountAsItWas) {
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
  EXPECT_EQ(storage->edge_count_.load(), before);
  auto reader = Reader(kReadCommitted);
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
  EXPECT_EQ(Entries(*reader, v, ms::EdgeDirection::IN, ms::View::NEW), 0U);
}

TEST_F(LinkEdgeMarkerTest, ACommitThenCollectionLeavesTheEdgeCountAsItWas) {
  auto const before = storage->edge_count_.load();
  auto writer = Writer();
  ASSERT_TRUE(LinkBoth(*writer));
  Commit(*writer);
  writer.reset();
  static_cast<ms::Storage &>(*storage).FreeMemory();
  EXPECT_EQ(storage->edge_count_.load(), before);
}

// OUT links, IN is refused by a concurrent writer of the other overlay: the transaction can only abort, balanced.
TEST_F(LinkEdgeMarkerTest, AHalfLinkedPairAbortsBalanced) {
  auto const before = storage->edge_count_.load();
  auto blocker = Writer();
  ASSERT_TRUE(Link(*blocker, ms::EdgeDirection::IN, v, v));
  auto writer = Writer();
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::OUT, u, v));
  EXPECT_FALSE(Link(*writer, ms::EdgeDirection::IN, v, u));
  EXPECT_TRUE(writer->TransactionHasSerializationError());
  writer->Abort();
  blocker->Abort();

  EXPECT_EQ(storage->edge_count_.load(), before);
  auto reader = Reader(kReadCommitted);
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::OUT, ms::View::NEW), 0U);
  EXPECT_EQ(Entries(*reader, v, ms::EdgeDirection::IN, ms::View::NEW), 0U);
}

TEST_F(LinkEdgeMarkerTest, ASecondMarkerOnOneOverlayConflicts) {
  auto first = Writer();
  auto second = Writer();
  ASSERT_TRUE(Link(*first, ms::EdgeDirection::OUT, u, v));
  EXPECT_FALSE(Link(*second, ms::EdgeDirection::OUT, u, v));
  EXPECT_TRUE(second->TransactionHasSerializationError());
  second->Abort();
  first->Abort();
}

TEST_F(LinkEdgeMarkerTest, AnEdgeCreatedOnAnOverlayConflictsWithAnInFlightMarkerInEitherOrder) {
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

TEST_F(LinkEdgeMarkerTest, ASelfLoopIsOneEntryPerList) {
  auto writer = Writer();
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::OUT, u, u));
  ASSERT_TRUE(Link(*writer, ms::EdgeDirection::IN, u, u));
  Commit(*writer);
  auto reader = Reader();
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::OUT, ms::View::NEW), 1U);
  EXPECT_EQ(Entries(*reader, u, ms::EdgeDirection::IN, ms::View::NEW), 1U);
}
