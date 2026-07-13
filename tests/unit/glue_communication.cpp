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

#include "communication/bolt/v1/value.hpp"
#include "glue/communication.hpp"
#include "helpers/stub_property_fga_checker.hpp"
#include "query/db_accessor.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

#ifdef MG_ENTERPRISE

namespace {

using StubPropertyFGAChecker = memgraph::tests::StubPropertyFGAChecker<memgraph::storage::Storage>;
using BoltValueType = memgraph::communication::bolt::Value::Type;

}  // namespace

class ToBoltTest : public ::testing::Test {
 public:
  std::filesystem::path data_directory{std::filesystem::temp_directory_path() / "MG_tests_unit_glue_communication"};

  memgraph::storage::Config config{[&]() {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    return config;
  }()};

  std::unique_ptr<memgraph::storage::Storage> storage{std::make_unique<memgraph::storage::InMemoryStorage>(config)};

  void SetUp() override { std::filesystem::create_directories(data_directory); }

  void TearDown() override { std::filesystem::remove_all(data_directory); }
};

TEST_F(ToBoltTest, PropertyFGAVertexDeniedPropertyOmitted) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});
  auto result = memgraph::glue::ToBoltVertex(vertex, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  ASSERT_EQ(props.size(), 1);
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_FALSE(props.contains("ssn"));
}

TEST_F(ToBoltTest, PropertyFGAVertexGrantedPropertyPresent) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  StubPropertyFGAChecker checker(storage.get(), {});
  auto result = memgraph::glue::ToBoltVertex(vertex, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  ASSERT_EQ(props.size(), 2);
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_EQ(props.at("ssn").ValueString(), "123-45-6789");
}

TEST_F(ToBoltTest, PropertyFGANullCheckerMeansNoFiltering) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto result = memgraph::glue::ToBoltVertex(vertex, *storage, memgraph::storage::View::NEW, nullptr);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  ASSERT_EQ(props.size(), 2);
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_EQ(props.at("ssn").ValueString(), "123-45-6789");
}

TEST_F(ToBoltTest, PropertyFGAEdgeDeniedPropertyOmitted) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto v1 = acc->CreateVertex();
  auto v2 = acc->CreateVertex();
  auto edge = acc->CreateEdge(&v1, &v2, acc->NameToEdgeType("PAID"));
  ASSERT_TRUE(edge.has_value());
  ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("amount"), memgraph::storage::PropertyValue(100)).has_value());
  ASSERT_TRUE(edge->SetProperty(acc->NameToProperty("secret"), memgraph::storage::PropertyValue("hidden")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  StubPropertyFGAChecker checker(storage.get(), {{"PAID", "secret"}});
  auto result = memgraph::glue::ToBoltEdge(*edge, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  ASSERT_EQ(props.size(), 1);
  EXPECT_EQ(props.at("amount").ValueInt(), 100);
  EXPECT_FALSE(props.contains("secret"));
}

TEST_F(ToBoltTest, PropertyFGAMultiLabelDenyOnAnyLabelDenies) {
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Person")).has_value());
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Bob")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("999")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});
  auto result = memgraph::glue::ToBoltVertex(vertex, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  ASSERT_EQ(props.size(), 1);
  EXPECT_EQ(props.at("name").ValueString(), "Bob");
  EXPECT_FALSE(props.contains("ssn"));
}

TEST_F(ToBoltTest, PropertyFGAOverlayOmitsDeniedOriginProperty) {
  // An overlay node reads its origin's properties through; a denied origin property is omitted over
  // Bolt exactly as it is for the real origin vertex.
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  memgraph::query::VirtualNode overlay({"Employee"}, {}, {}, std::optional<memgraph::query::VertexAccessor>{vertex});
  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});
  auto result = memgraph::glue::ToBoltVertex(overlay, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_FALSE(props.contains("ssn"));
}

TEST_F(ToBoltTest, PropertyFGAOverlayBoundOverrideExemptFromOriginDeny) {
  // An overlay-bound override is the author's computed value, not the origin's protected data, so it
  // is serialized even when its key name is denied on the origin's label.
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  auto ssn_id = acc->NameToProperty("ssn");
  memgraph::query::VirtualNode overlay({"Employee"},
                                       {{ssn_id, memgraph::storage::PropertyValue("masked")}},
                                       {},
                                       std::optional<memgraph::query::VertexAccessor>{vertex},
                                       {},
                                       memgraph::query::VirtualNode::key_set{ssn_id});
  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});
  auto result = memgraph::glue::ToBoltVertex(overlay, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_EQ(props.at("ssn").ValueString(), "masked");
}

TEST_F(ToBoltTest, PropertyFGASyntheticNodeNotFiltered) {
  // A synthetic node has no origin and mints no real-graph data, so property FGA does not apply.
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto ssn_id = acc->NameToProperty("ssn");
  memgraph::query::VirtualNode synthetic({"Employee"}, {{ssn_id, memgraph::storage::PropertyValue("x")}});
  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});
  auto result = memgraph::glue::ToBoltVertex(synthetic, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());

  auto const &props = result->properties;
  EXPECT_EQ(props.at("ssn").ValueString(), "x");
}

TEST_F(ToBoltTest, ToBoltVirtualGraphSerializesNodesAndEdges) {
  // A whole projection value returned to a client (RETURN g) serializes as a map of node and edge
  // lists. This path (ToBoltVirtualGraph) is distinct from consuming the value via USE or a procedure.
  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  auto g1 = vg.InsertNode(memgraph::query::VirtualNode({"N"}, {})).Gid();
  auto g2 = vg.InsertNode(memgraph::query::VirtualNode({"N"}, {})).Gid();
  vg.InsertEdgeIfNew(memgraph::query::VirtualEdge(vg.FindNode(g1), vg.FindNode(g2), "R"));

  auto result = memgraph::glue::ToBoltVirtualGraph(vg, *storage, memgraph::storage::View::NEW, nullptr);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->at("nodes").ValueList().size(), 2);
  ASSERT_EQ(result->at("edges").ValueList().size(), 1);
  EXPECT_EQ(result->at("edges").ValueList()[0].ValueEdge().type, "R");
}

TEST_F(ToBoltTest, ToBoltVirtualGraphRedactsDeniedOverlayProperty) {
  // A projection returned whole (RETURN g) must redact a denied origin property on an overlay member,
  // just as returning that node directly does - the FGA path threads through ToBoltVirtualGraph.
  auto acc = storage->Access(memgraph::storage::WRITE);
  auto vertex = acc->CreateVertex();
  ASSERT_TRUE(vertex.AddLabel(acc->NameToLabel("Employee")).has_value());
  ASSERT_TRUE(vertex.SetProperty(acc->NameToProperty("name"), memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(
      vertex.SetProperty(acc->NameToProperty("ssn"), memgraph::storage::PropertyValue("123-45-6789")).has_value());
  ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());

  memgraph::query::VirtualGraph vg(memgraph::utils::NewDeleteResource());
  vg.InsertNode(
      memgraph::query::VirtualNode({"Employee"}, {}, {}, std::optional<memgraph::query::VertexAccessor>{vertex}));
  StubPropertyFGAChecker checker(storage.get(), {{"Employee", "ssn"}});

  auto result = memgraph::glue::ToBoltVirtualGraph(vg, *storage, memgraph::storage::View::NEW, &checker);
  ASSERT_TRUE(result.has_value());
  auto const &nodes = result->at("nodes").ValueList();
  ASSERT_EQ(nodes.size(), 1);
  auto const &props = nodes[0].ValueVertex().properties;
  EXPECT_EQ(props.at("name").ValueString(), "Alice");
  EXPECT_FALSE(props.contains("ssn"));
}

#endif
