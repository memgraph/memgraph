// Copyright 2021 Memgraph Ltd.
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

#include "storage/v2/name_id_mapper.hpp"

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(NameIdMapper, Basic) {
  storage::NameIdMapper mapper;

  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.NameToId("n3"), 2);

  ASSERT_EQ(mapper.IdToName(0), "n1");
  ASSERT_EQ(mapper.IdToName(1), "n2");
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST(NameIdMapper, Correctness) {
  storage::NameIdMapper mapper;

  ASSERT_DEATH(mapper.IdToName(0), "");
  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.IdToName(0), "n1");

  ASSERT_DEATH(mapper.IdToName(1), "");
  ASSERT_EQ(mapper.NameToId("n2"), 1);
  ASSERT_EQ(mapper.IdToName(1), "n2");

  ASSERT_EQ(mapper.NameToId("n1"), 0);
  ASSERT_EQ(mapper.NameToId("n2"), 1);

  ASSERT_EQ(mapper.IdToName(1), "n2");
  ASSERT_EQ(mapper.IdToName(0), "n1");
}
