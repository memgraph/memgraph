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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "utils/on_scope_exit.hpp"

TEST(OnScopeExit, BasicUsage) {
  int variable = 1;
  {
    ASSERT_EQ(variable, 1);
    utils::OnScopeExit on_exit([&variable] { variable = 2; });
    EXPECT_EQ(variable, 1);
  }
  EXPECT_EQ(variable, 2);
}
