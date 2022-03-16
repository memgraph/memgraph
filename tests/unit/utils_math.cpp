// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cmath>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils/math.hpp"

TEST(UtilsMath, Log2) {
  for (uint64_t i = 1; i < 1000000; ++i) {
    ASSERT_EQ(memgraph::utils::Log2(i), static_cast<uint64_t>(log2(i)));
  }
}
