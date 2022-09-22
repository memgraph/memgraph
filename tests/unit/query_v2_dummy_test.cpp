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

#include <iterator>
#include <memory>
#include <variant>
#include <vector>

#include "common/types.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/context.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/plan/operator.hpp"

#include "query/v2/plan/operator.hpp"
#include "query_v2_query_plan_common.hpp"

class Dummy : public testing::Test {
 protected:
  void SetUp() override {}
};

TEST_F(Dummy, DummyTest) { ASSERT_EQ(true, true); }
