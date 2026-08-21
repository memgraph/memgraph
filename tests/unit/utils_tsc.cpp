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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "utils/tsc.hpp"

using memgraph::utils::IsAvailableTSC;
using memgraph::utils::ReadTSC;
using memgraph::utils::ReadTSCEnd;
using memgraph::utils::ReadTSCStart;

namespace {

uint64_t TicksWhileSleeping(std::chrono::milliseconds duration) {
  auto const start = ReadTSCStart();
  std::this_thread::sleep_for(duration);
  return ReadTSCEnd() - start;
}

}  // namespace

TEST(TSC, AvailabilityIsStable) {
  auto const available = IsAvailableTSC();
  EXPECT_EQ(available, IsAvailableTSC());
}

TEST(TSC, ReadsNeverGoBackwards) {
  if (!IsAvailableTSC()) GTEST_SKIP() << "no invariant TSC on this machine";

  auto previous = ReadTSCStart();
  for (int i = 0; i < 1000; ++i) {
    auto const current = ReadTSC();
    EXPECT_GE(current, previous);
    previous = current;
  }
  EXPECT_GE(ReadTSCEnd(), previous);
}

TEST(TSC, TicksTrackElapsedTime) {
  if (!IsAvailableTSC()) GTEST_SKIP() << "no invariant TSC on this machine";

  // The counter has no advertised frequency here, so the only thing worth
  // asserting is that a longer wait yields proportionally more ticks.
  auto const brief = TicksWhileSleeping(std::chrono::milliseconds{5});
  auto const lengthy = TicksWhileSleeping(std::chrono::milliseconds{50});

  EXPECT_GT(brief, 0);
  EXPECT_GT(lengthy, brief * 2);
}
