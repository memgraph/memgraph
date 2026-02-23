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

#include "storage/v2/schema_info_types.hpp"

#include <vector>

using memgraph::storage::AssignLabels;
using memgraph::storage::ContainsLabel;
using memgraph::storage::LabelId;
using memgraph::storage::RemoveLabel;
using memgraph::storage::ToVertexKey;
using memgraph::storage::VertexKey;
using memgraph::utils::PackedVarintVector;

// --- ContainsLabel ---

TEST(SchemaInfoTypes, ContainsLabelEmpty) {
  PackedVarintVector labels;
  EXPECT_FALSE(ContainsLabel(labels, LabelId::FromUint(1)));
}

TEST(SchemaInfoTypes, ContainsLabelFound) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(5);
  labels.push_back(10);
  EXPECT_TRUE(ContainsLabel(labels, LabelId::FromUint(5)));
}

TEST(SchemaInfoTypes, ContainsLabelNotFound) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(5);
  EXPECT_FALSE(ContainsLabel(labels, LabelId::FromUint(3)));
}

TEST(SchemaInfoTypes, ContainsLabelFirstAndLast) {
  PackedVarintVector labels;
  labels.push_back(10);
  labels.push_back(20);
  labels.push_back(30);
  EXPECT_TRUE(ContainsLabel(labels, LabelId::FromUint(10)));
  EXPECT_TRUE(ContainsLabel(labels, LabelId::FromUint(30)));
}

// --- RemoveLabel ---

TEST(SchemaInfoTypes, RemoveLabelFromMiddle) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(2);
  labels.push_back(3);
  RemoveLabel(labels, LabelId::FromUint(2));
  auto key = ToVertexKey(labels);
  EXPECT_EQ(key.size(), 2);
  EXPECT_EQ(key[0], LabelId::FromUint(1));
  EXPECT_EQ(key[1], LabelId::FromUint(3));
}

TEST(SchemaInfoTypes, RemoveLabelFromFront) {
  PackedVarintVector labels;
  labels.push_back(10);
  labels.push_back(20);
  RemoveLabel(labels, LabelId::FromUint(10));
  auto key = ToVertexKey(labels);
  ASSERT_EQ(key.size(), 1);
  EXPECT_EQ(key[0], LabelId::FromUint(20));
}

TEST(SchemaInfoTypes, RemoveLabelFromEnd) {
  PackedVarintVector labels;
  labels.push_back(10);
  labels.push_back(20);
  RemoveLabel(labels, LabelId::FromUint(20));
  auto key = ToVertexKey(labels);
  ASSERT_EQ(key.size(), 1);
  EXPECT_EQ(key[0], LabelId::FromUint(10));
}

TEST(SchemaInfoTypes, RemoveLabelNotPresent) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(2);
  RemoveLabel(labels, LabelId::FromUint(99));
  EXPECT_EQ(labels.count(), 2);
}

TEST(SchemaInfoTypes, RemoveLabelOnlyOne) {
  PackedVarintVector labels;
  labels.push_back(5);
  RemoveLabel(labels, LabelId::FromUint(5));
  EXPECT_TRUE(labels.empty());
}

TEST(SchemaInfoTypes, RemoveLabelDuplicateRemovesOnlyFirst) {
  PackedVarintVector labels;
  labels.push_back(3);
  labels.push_back(3);
  labels.push_back(3);
  RemoveLabel(labels, LabelId::FromUint(3));
  EXPECT_EQ(labels.count(), 2);
  for (auto val : labels) {
    EXPECT_EQ(val, 3);
  }
}

// --- ToVertexKey ---

TEST(SchemaInfoTypes, ToVertexKeyEmpty) {
  PackedVarintVector labels;
  auto key = ToVertexKey(labels);
  EXPECT_TRUE(key.empty());
}

TEST(SchemaInfoTypes, ToVertexKeyRoundTrip) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(128);
  labels.push_back(UINT32_MAX);
  auto key = ToVertexKey(labels);
  ASSERT_EQ(key.size(), 3);
  EXPECT_EQ(key[0], LabelId::FromUint(1));
  EXPECT_EQ(key[1], LabelId::FromUint(128));
  EXPECT_EQ(key[2], LabelId::FromUint(UINT32_MAX));
}

// --- AssignLabels ---

TEST(SchemaInfoTypes, AssignLabelsEmpty) {
  PackedVarintVector dest;
  dest.push_back(99);
  VertexKey src;
  AssignLabels(dest, src);
  EXPECT_TRUE(dest.empty());
}

TEST(SchemaInfoTypes, AssignLabelsRoundTrip) {
  VertexKey src;
  src.push_back(LabelId::FromUint(5));
  src.push_back(LabelId::FromUint(200));
  src.push_back(LabelId::FromUint(1000000));

  PackedVarintVector dest;
  AssignLabels(dest, src);

  auto result = ToVertexKey(dest);
  ASSERT_EQ(result.size(), src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    EXPECT_EQ(result[i], src[i]);
  }
}

TEST(SchemaInfoTypes, AssignLabelsOverwrites) {
  PackedVarintVector dest;
  dest.push_back(1);
  dest.push_back(2);
  dest.push_back(3);

  VertexKey src;
  src.push_back(LabelId::FromUint(10));

  AssignLabels(dest, src);
  EXPECT_EQ(dest.count(), 1);
  EXPECT_EQ(*dest.begin(), 10);
}

// --- Combined operations ---

TEST(SchemaInfoTypes, RemoveThenContains) {
  PackedVarintVector labels;
  labels.push_back(1);
  labels.push_back(2);
  labels.push_back(3);
  RemoveLabel(labels, LabelId::FromUint(2));
  EXPECT_FALSE(ContainsLabel(labels, LabelId::FromUint(2)));
  EXPECT_TRUE(ContainsLabel(labels, LabelId::FromUint(1)));
  EXPECT_TRUE(ContainsLabel(labels, LabelId::FromUint(3)));
}

TEST(SchemaInfoTypes, AssignThenRemoveThenToVertexKey) {
  VertexKey src;
  src.push_back(LabelId::FromUint(10));
  src.push_back(LabelId::FromUint(20));
  src.push_back(LabelId::FromUint(30));

  PackedVarintVector labels;
  AssignLabels(labels, src);
  RemoveLabel(labels, LabelId::FromUint(20));

  auto key = ToVertexKey(labels);
  ASSERT_EQ(key.size(), 2);
  EXPECT_EQ(key[0], LabelId::FromUint(10));
  EXPECT_EQ(key[1], LabelId::FromUint(30));
}
