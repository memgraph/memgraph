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

#include "utils/indirect.hpp"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

using memgraph::utils::indirect;

namespace {

// A type that holds itself through `indirect`, which is what the type is for.
struct Tree {
  std::string name;
  std::vector<indirect<Tree>> children;
};

}  // namespace

TEST(UtilsIndirect, CopyIsDeep) {
  indirect<std::string> original(std::in_place, "a");
  indirect<std::string> copy = original;
  *copy += "b";
  EXPECT_EQ(*original, "a");
  EXPECT_EQ(*copy, "ab");

  indirect<std::string> assigned(std::string{"x"});
  assigned = original;
  *assigned += "c";
  EXPECT_EQ(*original, "a");
  EXPECT_EQ(*assigned, "ac");
}

TEST(UtilsIndirect, MoveLeavesTheSourceValueless) {
  indirect<std::string> source(std::string{"a"});
  indirect<std::string> target = std::move(source);
  EXPECT_TRUE(source.valueless_after_move());  // NOLINT(bugprone-use-after-move)
  EXPECT_FALSE(target.valueless_after_move());
  EXPECT_EQ(*target, "a");
  EXPECT_EQ(target->size(), 1U);
}

TEST(UtilsIndirect, HoldsAnIncompleteSelf) {
  Tree root{.name = "root", .children = {}};
  root.children.emplace_back(Tree{.name = "leaf", .children = {}});
  Tree copy = root;
  copy.children.front()->name = "changed";
  EXPECT_EQ(root.children.front()->name, "leaf");
  EXPECT_EQ(copy.children.front()->name, "changed");
}

TEST(UtilsIndirect, Swap) {
  indirect<int> lhs(1);
  indirect<int> rhs(2);
  swap(lhs, rhs);
  EXPECT_EQ(*lhs, 2);
  EXPECT_EQ(*rhs, 1);
}
