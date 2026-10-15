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

// Stopping a storage waits for the async indexer's worker to finish, so a wake-up that reaches that
// worker only some of the time is enough to hang an instance on shutdown or on a dropped database.
// The window is at the worker's own startup, between reading its wait condition and registering to
// be woken, and it is only a few instructions wide. Nothing here indexes anything: the test starts
// and stops a worker that has no work, which is the shortest path to that window.

#include <gtest/gtest.h>

#include <stop_token>

#include "storage/v2/async_indexer.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace {

// A stopped indexer reports itself stopped for good, so each round needs a fresh one. The count is
// set from how far the window actually reaches, measured against the defect this guards: at a tenth
// of this it caught it in four runs out of ten, so a tenth is not enough to rely on a single run.
// Correct code pays a fixed dozen seconds or so for it.
constexpr int kRounds = 400'000;

}  // namespace

// The deadlock this guards against presents as the test not finishing, since a lost wake-up leaves
// both the worker and the thread waiting for it asleep. That is the honest shape for a deadlock,
// and the unit-test timeout turns it into a named failure with a core rather than an unattributed
// cancellation. The assertion in the loop is not what catches it; see the note there.
TEST(AsyncIndexer, ShutdownCompletesWhenTheWorkerIsStillStarting) {
  memgraph::storage::Config config{};
  // Nothing in this test needs collection, and a periodic pass would only add unrelated threads.
  config.gc.type = memgraph::storage::Config::Gc::Type::NONE;
  memgraph::storage::InMemoryStorage storage{config};

  for (int round = 0; round < kRounds; ++round) {
    std::stop_source stop_source;
    memgraph::storage::AsyncIndexer indexer;
    indexer.Start(stop_source.get_token(), &storage);
    indexer.Shutdown();
    // Shutdown waits on exactly this condition before returning, so it cannot fail while Shutdown
    // keeps that promise. It is here for the regression where Shutdown stops waiting: a worker
    // outliving the call is then reported here rather than as a crash somewhere later.
    ASSERT_TRUE(indexer.HasThreadStopped())
        << "Shutdown returned on round " << round << " while the worker was still running";
  }
}
