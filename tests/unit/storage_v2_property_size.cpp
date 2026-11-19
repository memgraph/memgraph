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

#include <gtest/gtest.h>

#include <memory>
#include <stop_token>
#include <thread>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::PropertyValue;
using memgraph::storage::Storage;
using memgraph::storage::View;

namespace {

struct OneEdge {
  Gid from;
  Gid to;
};

auto MakeStore() -> std::unique_ptr<Storage> {
  return std::make_unique<InMemoryStorage>(
      memgraph::storage::Config{.salient = {.items = {.properties_on_edges = true}}});
}

auto CreateEdgeWithProperty(Storage &store, PropertyValue value) -> OneEdge {
  auto acc = store.Access(memgraph::storage::WRITE);
  auto from = acc->CreateVertex();
  auto to = acc->CreateVertex();
  auto edge = acc->CreateEdge(&from, &to, acc->NameToEdgeType("link"));
  EXPECT_TRUE(edge.has_value());
  EXPECT_TRUE(edge->SetProperty(acc->NameToProperty("size_me"), std::move(value)).has_value());
  auto const result = OneEdge{.from = from.Gid(), .to = to.Gid()};
  EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  return result;
}

auto FindEdge(Storage::Accessor &acc, OneEdge const &ids) -> memgraph::storage::EdgeAccessor {
  auto from = acc.FindVertex(ids.from, View::NEW);
  EXPECT_TRUE(from.has_value());
  auto out = from->OutEdges(View::NEW);
  EXPECT_TRUE(out.has_value());
  EXPECT_EQ(out->edges.size(), 1);
  return out->edges[0];
}

}  // namespace

// An edge modified in this transaction answers the size its committed self answers, so the branch
// that reconstructs the value from the delta chain agrees with the one that reads the record.
TEST(EdgePropertySize, AgreesWhetherOrNotTheEdgeCarriesADelta) {
  auto store = MakeStore();
  auto const ids = CreateEdgeWithProperty(*store, PropertyValue{"a string worth measuring"});

  auto size_with_a_delta = uint64_t{0};
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto edge = FindEdge(*acc, ids);
    auto const property = acc->NameToProperty("size_me");
    ASSERT_TRUE(edge.SetProperty(property, PropertyValue{"a different string"}).has_value());

    auto const size = edge.GetPropertySize(property, View::NEW);
    ASSERT_TRUE(size.has_value());
    size_with_a_delta = *size;
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  auto acc = store->Access(memgraph::storage::READ);
  auto edge = FindEdge(*acc, ids);
  auto const size = edge.GetPropertySize(acc->NameToProperty("size_me"), View::NEW);
  ASSERT_TRUE(size.has_value());
  EXPECT_EQ(*size, size_with_a_delta);
  EXPECT_GT(*size, 0);
}

// Reading an edge's property size reads the property itself, and the edge's lock admits no new
// reader once a writer is waiting. A read holding that lock across the nested read would wait for
// the writer while the writer waited for it, so finishing at all is what this asserts.
TEST(EdgePropertySize, AnswersWhileTheEdgeIsBeingWritten) {
  auto store = MakeStore();
  auto const ids = CreateEdgeWithProperty(*store, PropertyValue{"measure me"});

  // A delta on the edge is what sends the read down the branch that reads the property again.
  {
    auto acc = store->Access(memgraph::storage::WRITE);
    auto edge = FindEdge(*acc, ids);
    ASSERT_TRUE(edge.SetProperty(acc->NameToProperty("size_me"), PropertyValue{"measured once"}).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  constexpr auto kReaders = 4;
  constexpr auto kReadsEach = 300;

  // Each write is its own transaction, so the delta chain the readers walk stays short and the
  // contention being tested is for the edge's lock rather than for the length of its history. The
  // writer runs until it is asked to stop, which every jthread here is on the way out of the
  // scope, so neither thread waits on the other to finish first.
  auto writer = std::jthread{[&](std::stop_token const stop) {
    for (auto next = int64_t{0}; !stop.stop_requested(); ++next) {
      auto acc = store->Access(memgraph::storage::WRITE);
      auto edge = FindEdge(*acc, ids);
      // A writer that gives up leaves the readers uncontended, which is the one way this finishes
      // while the defect is present, so it fails rather than returning quietly.
      if (!edge.SetProperty(acc->NameToProperty("size_me"), PropertyValue{next}).has_value()) {
        ADD_FAILURE() << "the writer could not set the property, so the readers were never contended";
        return;
      }
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) {
        ADD_FAILURE() << "the writer could not commit, so the readers were never contended";
        return;
      }
    }
  }};

  auto readers = std::vector<std::jthread>{};
  readers.reserve(kReaders);
  for (auto reader = 0; reader != kReaders; ++reader) {
    readers.emplace_back([&] {
      for (auto read = 0; read != kReadsEach; ++read) {
        auto acc = store->Access(memgraph::storage::READ);
        auto edge = FindEdge(*acc, ids);
        auto const size = edge.GetPropertySize(acc->NameToProperty("size_me"), View::NEW);
        EXPECT_TRUE(size.has_value());
      }
    });
  }
}
