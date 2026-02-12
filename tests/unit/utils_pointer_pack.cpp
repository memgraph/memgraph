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

#include <cstdint>

#include "gtest/gtest.h"

#include "utils/pointer_pack.hpp"

using memgraph::utils::PointerPack;

namespace {

struct alignas(8) Dummy {
  uint64_t value{0};
};

}  // namespace

class PointerPackTest : public ::testing::Test {
 protected:
  Dummy a{1};
  Dummy b{2};
};

TEST_F(PointerPackTest, DefaultConstructedIsNull) {
  PointerPack<Dummy, 2> pp;
  EXPECT_EQ(pp.get_ptr(), nullptr);
  EXPECT_EQ(pp.get<0>(), 0u);
  EXPECT_EQ(pp.get<1>(), 0u);
}

TEST_F(PointerPackTest, ConstructWithPointerOnly) {
  PointerPack<Dummy, 2> pp(&a);
  EXPECT_EQ(pp.get_ptr(), &a);
  EXPECT_EQ(pp.get<0>(), 0u);
  EXPECT_EQ(pp.get<1>(), 0u);
}

TEST_F(PointerPackTest, ConstructWithPointerAndFlags) {
  PointerPack<Dummy, 3> pp(&a, 0b101);
  EXPECT_EQ(pp.get_ptr(), &a);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 0u);
  EXPECT_EQ(pp.get<2>(), 1u);
}

TEST_F(PointerPackTest, SetAndGetIndividualBits) {
  PointerPack<Dummy, 3> pp(&a);

  pp.set<0>(1);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 0u);
  EXPECT_EQ(pp.get<2>(), 0u);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<1>(1);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
  EXPECT_EQ(pp.get<2>(), 0u);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<2>(1);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
  EXPECT_EQ(pp.get<2>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<0>(0);
  EXPECT_EQ(pp.get<0>(), 0u);
  EXPECT_EQ(pp.get<1>(), 1u);
  EXPECT_EQ(pp.get<2>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);
}

TEST_F(PointerPackTest, MultiBitField) {
  PointerPack<Dummy, 3> pp(&a);

  pp.set<0, 2>(0b11);
  EXPECT_EQ((pp.get<0, 2>()), 0b11u);
  EXPECT_EQ(pp.get<2>(), 0u);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<0, 2>(0b10);
  EXPECT_EQ((pp.get<0, 2>()), 0b10u);

  pp.set<0, 3>(0b101);
  EXPECT_EQ((pp.get<0, 3>()), 0b101u);
}

TEST_F(PointerPackTest, SetPtrPreservesFlags) {
  PointerPack<Dummy, 2> pp(&a);
  pp.set<0>(1);
  pp.set<1>(1);

  pp.set_ptr(&b);
  EXPECT_EQ(pp.get_ptr(), &b);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
}

TEST_F(PointerPackTest, SetFlagsPreservesPointer) {
  PointerPack<Dummy, 2> pp(&a);

  pp.set<0>(1);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<1>(1);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<0>(0);
  EXPECT_EQ(pp.get_ptr(), &a);
}

TEST_F(PointerPackTest, ImplicitConversionToPointer) {
  PointerPack<Dummy, 2> pp(&a);
  pp.set<0>(1);

  Dummy *raw = pp;
  EXPECT_EQ(raw, &a);
}

TEST_F(PointerPackTest, AssignmentFromPointer) {
  PointerPack<Dummy, 2> pp(&a);
  pp.set<0>(1);
  pp.set<1>(1);

  pp = &b;
  EXPECT_EQ(pp.get_ptr(), &b);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
}

TEST_F(PointerPackTest, NullPointerWithFlags) {
  PointerPack<Dummy, 2> pp(nullptr, 0b11);
  EXPECT_EQ(pp.get_ptr(), nullptr);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
}

TEST_F(PointerPackTest, ClearFlagsIndependently) {
  PointerPack<Dummy, 3> pp(&a, 0b111);

  pp.set<1>(0);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 0u);
  EXPECT_EQ(pp.get<2>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);
}

TEST_F(PointerPackTest, FlagsExcessBitsMasked) {
  PointerPack<Dummy, 2> pp(&a);
  pp.set<0>(0xFF);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);
}

TEST_F(PointerPackTest, ConstructorFlagsExcessBitsMasked) {
  PointerPack<Dummy, 2> pp(&a, 0xFF);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get<1>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);
}

TEST_F(PointerPackTest, SingleBitPack) {
  PointerPack<Dummy, 1> pp(&a);
  EXPECT_EQ(pp.get<0>(), 0u);

  pp.set<0>(1);
  EXPECT_EQ(pp.get<0>(), 1u);
  EXPECT_EQ(pp.get_ptr(), &a);

  pp.set<0>(0);
  EXPECT_EQ(pp.get<0>(), 0u);
  EXPECT_EQ(pp.get_ptr(), &a);
}
