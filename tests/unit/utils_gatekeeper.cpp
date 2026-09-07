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

#include "utils/gatekeeper.hpp"

using namespace memgraph::utils;
using namespace std::chrono_literals;

namespace {

struct Widget {
  int v;
};

}  // namespace

// An accessor that is on its way out must not deny exclusive access: the caller
// asked for sole access, and it gets it as soon as the other holder lets go.
TEST(UtilsGatekeeper, TryExclusivelyWaitsForAnotherAccessorToDrop) {
  auto gk = Gatekeeper<Widget>{42};

  auto sole = gk.access();
  ASSERT_TRUE(sole.has_value());

  auto other = gk.access();
  ASSERT_TRUE(other.has_value());

  auto releaser = std::jthread{[acc = std::move(*other)]() mutable {
    std::this_thread::sleep_for(50ms);
    acc.reset();
  }};

  auto observed = 0;
  EXPECT_TRUE(static_cast<bool>(sole->try_exclusively([&](Widget &w) { observed = w.v; }, 5s)));
  EXPECT_EQ(observed, 42);
}

// A holder that stays put is a genuine denial, reported within the timeout.
TEST(UtilsGatekeeper, TryExclusivelyGivesUpWhenAnotherAccessorStays) {
  auto gk = Gatekeeper<Widget>{42};

  auto sole = gk.access();
  ASSERT_TRUE(sole.has_value());
  auto other = gk.access();
  ASSERT_TRUE(other.has_value());

  auto ran = false;
  EXPECT_FALSE(static_cast<bool>(sole->try_exclusively([&](Widget & /*unused*/) { ran = true; }, 20ms)));
  EXPECT_FALSE(ran);
}

// Zero timeout keeps the one-shot check available for callers that must not block.
TEST(UtilsGatekeeper, TryExclusivelyWithZeroTimeoutSamplesOnce) {
  auto gk = Gatekeeper<Widget>{42};

  auto sole = gk.access();
  ASSERT_TRUE(sole.has_value());

  {
    auto other = gk.access();
    ASSERT_TRUE(other.has_value());
    EXPECT_FALSE(static_cast<bool>(sole->try_exclusively([](Widget & /*unused*/) {}, 0ms)));
  }

  EXPECT_TRUE(static_cast<bool>(sole->try_exclusively([](Widget & /*unused*/) {}, 0ms)));
}

TEST(UtilsGatekeeper, TryExclusivelyReturnsTheFunctionsResult) {
  auto gk = Gatekeeper<Widget>{42};

  auto sole = gk.access();
  ASSERT_TRUE(sole.has_value());

  auto result = sole->try_exclusively([](Widget &w) { return w.v + 1; }, 0ms);
  ASSERT_TRUE(static_cast<bool>(result));
  EXPECT_EQ(result.value(), 43);
}

// The wait must not swallow the exception the function throws: the storage-mode
// switch reports its refusals that way.
TEST(UtilsGatekeeper, TryExclusivelyPropagatesExceptions) {
  auto gk = Gatekeeper<Widget>{42};

  auto sole = gk.access();
  ASSERT_TRUE(sole.has_value());

  EXPECT_THROW((void)sole->try_exclusively([](Widget & /*unused*/) { throw std::runtime_error{"nope"}; }, 0ms),
               std::runtime_error);
}
