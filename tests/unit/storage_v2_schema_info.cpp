// Copyright 2024 Memgraph Ltd.
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
#include <boost/asio/execution/prefer_only.hpp>

#include "storage/v2/schema_info.hpp"

// NOLINTNEXTLINE(google-build-using-namespace)
using namespace memgraph::storage;

TEST(PropertyInfo, DefaultConstruction) {
  PropertyInfo propInfo;
  EXPECT_TRUE(propInfo.property_types.empty());
  EXPECT_EQ(propInfo.number_of_property_occurrences, 0);
}

TEST(PropertyInfo, MergeWithEmptyStats) {
  PropertyInfo propInfo;
  propInfo.number_of_property_occurrences = 1;
  propInfo.property_types[SchemaPropertyType::Integer] = 1;

  PropertyInfo propInfo2;
  propInfo.MergeStats(std::move(propInfo2));

  EXPECT_EQ(propInfo.property_types.size(), 1);
  EXPECT_EQ(propInfo.property_types[SchemaPropertyType::Integer], 1);
  EXPECT_EQ(propInfo.number_of_property_occurrences, 1);
}

TEST(PropertyInfo, MergeWithDifferentTypeStats) {
  PropertyInfo propInfo;
  propInfo.number_of_property_occurrences = 1;
  propInfo.property_types[SchemaPropertyType::Integer] = 1;

  PropertyInfo propInfo2;
  propInfo2.number_of_property_occurrences = 1;
  propInfo2.property_types[SchemaPropertyType::String] = 1;

  propInfo.MergeStats(std::move(propInfo2));

  EXPECT_EQ(propInfo.number_of_property_occurrences, 2);
  EXPECT_EQ(propInfo.property_types.size(), 2);
  EXPECT_EQ(propInfo.property_types[SchemaPropertyType::Integer], 1);
  EXPECT_EQ(propInfo.property_types[SchemaPropertyType::String], 1);
}

TEST(PropertyInfo, MergeWithSameTypeStats) {
  PropertyInfo propInfo;
  propInfo.number_of_property_occurrences = 1;
  propInfo.property_types[SchemaPropertyType::Integer] = 1;

  PropertyInfo propInfo2;
  propInfo2.number_of_property_occurrences = 2;
  propInfo2.property_types[SchemaPropertyType::Integer] = 2;

  propInfo.MergeStats(std::move(propInfo2));

  EXPECT_EQ(propInfo.number_of_property_occurrences, 3);
  EXPECT_EQ(propInfo.property_types.size(), 1);
  EXPECT_EQ(propInfo.property_types[SchemaPropertyType::Integer], 3);
}

TEST(LabelsInfo, DefaultConstruction) {
  LabelsInfo labelsInfo;
  EXPECT_EQ(labelsInfo.number_of_label_occurrences, 0);
  EXPECT_TRUE(labelsInfo.properties.empty());
}

TEST(LabelsInfo, MergeWithEmptyStats) {
  LabelsInfo labelsInfo;
  labelsInfo.number_of_label_occurrences = 1;
  PropertyInfo propInfo;

  propInfo.number_of_property_occurrences = 1;
  labelsInfo.properties["prop1"].number_of_property_occurrences = 1;

  LabelsInfo labelsInfo2;

  labelsInfo.MergeStats(std::move(labelsInfo2));

  EXPECT_EQ(labelsInfo.number_of_label_occurrences, 1);
  EXPECT_EQ(labelsInfo.properties.size(), 1);
  EXPECT_EQ(labelsInfo.properties["prop1"].number_of_property_occurrences, 1);
}

TEST(LabelsInfo, MergeWithDifferentPropertyStats) {
  LabelsInfo labelsInfo;
  labelsInfo.number_of_label_occurrences = 1;
  PropertyInfo propInfo;

  propInfo.number_of_property_occurrences = 1;
  labelsInfo.properties["prop1"].number_of_property_occurrences = 1;

  LabelsInfo labelsInfo2;
  labelsInfo2.number_of_label_occurrences = 3;
  labelsInfo2.properties["prop2"].number_of_property_occurrences = 2;
  labelsInfo2.properties["prop3"].number_of_property_occurrences = 1;

  labelsInfo.MergeStats(std::move(labelsInfo2));

  EXPECT_EQ(labelsInfo.number_of_label_occurrences, 4);
  EXPECT_EQ(labelsInfo.properties.size(), 3);
  EXPECT_EQ(labelsInfo.properties["prop1"].number_of_property_occurrences, 1);
  EXPECT_EQ(labelsInfo.properties["prop2"].number_of_property_occurrences, 2);
  EXPECT_EQ(labelsInfo.properties["prop3"].number_of_property_occurrences, 1);
}

TEST(LabelsInfo, MergeWithSamePropertyStats) {
  LabelsInfo labelsInfo;
  labelsInfo.number_of_label_occurrences = 1;
  PropertyInfo propInfo;

  propInfo.number_of_property_occurrences = 1;
  labelsInfo.properties["prop1"].number_of_property_occurrences = 1;

  LabelsInfo labelsInfo2;
  labelsInfo2.number_of_label_occurrences = 2;
  labelsInfo2.properties["prop1"].number_of_property_occurrences = 2;

  labelsInfo.MergeStats(std::move(labelsInfo2));

  EXPECT_EQ(labelsInfo.number_of_label_occurrences, 3);
  EXPECT_EQ(labelsInfo.properties.size(), 1);
  EXPECT_EQ(labelsInfo.properties["prop1"].number_of_property_occurrences, 3);
}

TEST(LabelsInfo, MergeLabelsWithEmptyPropertyStats) {
  LabelsInfo labelsInfo;
  labelsInfo.number_of_label_occurrences = 1;

  LabelsInfo labelsInfo2;
  labelsInfo2.number_of_label_occurrences = 2;

  labelsInfo.MergeStats(std::move(labelsInfo2));

  EXPECT_EQ(labelsInfo.number_of_label_occurrences, 3);
  EXPECT_TRUE(labelsInfo.properties.empty());
}

TEST(NodesInfo, DefaultConstruction) {
  NodesInfo nodesInfo;
  EXPECT_TRUE(nodesInfo.node_types_properties.empty());
}

TEST(NodesInfo, MergeWithEmptyStats) {
  NodesInfo nodesInfo;
  NodesInfo::LabelsSet labelsSet;
  labelsSet.insert("lbl1");
  nodesInfo.node_types_properties.emplace(labelsSet, LabelsInfo());

  NodesInfo nodesInfo2;

  nodesInfo.MergeStats(std::move(nodesInfo2));

  EXPECT_EQ(nodesInfo.node_types_properties.size(), 1);
}

TEST(NodesInfo, MergeWithDifferentLabelsStats) {
  NodesInfo nodesInfo;
  NodesInfo::LabelsSet labelsSet1;
  labelsSet1.insert("lbl1");
  nodesInfo.node_types_properties.emplace(labelsSet1, LabelsInfo());

  NodesInfo nodesInfo2;
  NodesInfo::LabelsSet labelsSet2;
  labelsSet2.insert("lbl2");
  nodesInfo2.node_types_properties.emplace(labelsSet2, LabelsInfo());

  nodesInfo.MergeStats(std::move(nodesInfo2));

  EXPECT_EQ(nodesInfo.node_types_properties.size(), 2);
}

TEST(NodesInfo, MergeWithSameLabelsStats) {
  NodesInfo nodesInfo;
  NodesInfo::LabelsSet labelsSet1;
  labelsSet1.insert("lbl1");
  nodesInfo.node_types_properties.emplace(labelsSet1, LabelsInfo());

  NodesInfo nodesInfo2;
  nodesInfo2.node_types_properties.emplace(labelsSet1, LabelsInfo());

  nodesInfo.MergeStats(std::move(nodesInfo2));

  EXPECT_EQ(nodesInfo.node_types_properties.size(), 1);
}

TEST(SchemaInfo, DefaultConstruction) {
  SchemaInfo schemaInfo;
  EXPECT_TRUE(schemaInfo.nodes.node_types_properties.empty());
}

TEST(SchemaInfo, MergeWithEmptyStats) {
  SchemaInfo schemaInfo;
  NodesInfo::LabelsSet labelsSet1;
  labelsSet1.insert("lbl1");
  schemaInfo.nodes.node_types_properties.emplace(labelsSet1, LabelsInfo());

  SchemaInfo schemaInfo2;

  schemaInfo.MergeStats(std::move(schemaInfo2));

  EXPECT_EQ(schemaInfo.nodes.node_types_properties.size(), 1);
}

TEST(SchemaInfo, MergeWithNodeStats) {
  SchemaInfo schemaInfo;
  NodesInfo::LabelsSet labelsSet1;
  labelsSet1.insert("lbl1");
  schemaInfo.nodes.node_types_properties.emplace(labelsSet1, LabelsInfo());

  SchemaInfo schemaInfo2;
  NodesInfo::LabelsSet labelsSet2;
  labelsSet2.insert("lbl2");
  schemaInfo2.nodes.node_types_properties.emplace(labelsSet2, LabelsInfo());

  schemaInfo.MergeStats(std::move(schemaInfo2));

  EXPECT_EQ(schemaInfo.nodes.node_types_properties.size(), 2);
}
