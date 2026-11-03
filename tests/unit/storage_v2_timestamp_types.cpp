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

#include "storage/v2/timestamp_types.hpp"

namespace memgraph::storage {

template <typename L, typename R>
concept Orderable = requires(L l, R r) { l < r; };

TEST(TimestampTypes, TimestampsOfOneSpaceCompare) {
  static_assert(Orderable<StartTimestamp, StartTimestamp>);
  static_assert(Orderable<CommitTimestamp, CommitTimestamp>);
  static_assert(Orderable<DurableTimestamp, DurableTimestamp>);

  EXPECT_LT(StartTimestamp::FromUint(1), StartTimestamp::FromUint(2));
}

TEST(TimestampTypes, TimestampsOfDifferentSpacesDoNotCompare) {
  static_assert(!Orderable<StartTimestamp, CommitTimestamp>);
  static_assert(!Orderable<CommitTimestamp, DurableTimestamp>);
  static_assert(!Orderable<StartTimestamp, DurableTimestamp>);

  // A raw counter value belongs to no sequence, so it cannot stand in for one.
  static_assert(!Orderable<CommitTimestamp, uint64_t>);
}

}  // namespace memgraph::storage
