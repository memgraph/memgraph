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

#include <chrono>
#include <cmath>
#include <iterator>
#include <memory>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "coordinator/shard_map.hpp"
#include "exceptions.hpp"
#include "functions/awesome_memgraph_functions.hpp"
#include "parser/opencypher/parser.hpp"
#include "query/v2/accessors.hpp"
#include "query/v2/bindings/eval.hpp"
#include "query/v2/bindings/frame.hpp"
#include "query/v2/context.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/requests.hpp"
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

#include "query_v2_query_common.hpp"
//#include "utils/temporal.hpp"

using namespace memgraph::query::v2;
using memgraph::query::v2::test_common::ToIntList;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

using memgraph::io::Time;
using memgraph::io::TimedOut;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::msgs::CreateExpandRequest;
using memgraph::msgs::CreateExpandResponse;
using memgraph::msgs::CreateVerticesRequest;
using memgraph::msgs::CreateVerticesResponse;
using memgraph::msgs::ExpandOneRequest;
using memgraph::msgs::ExpandOneResponse;
using memgraph::msgs::ExpandOneResultRow;
using memgraph::msgs::NewExpand;
using memgraph::msgs::NewVertex;
using memgraph::msgs::ScanVerticesRequest;
using memgraph::msgs::ScanVerticesResponse;
using ShardMap = memgraph::coordinator::ShardMap;
using LabelId = memgraph::storage::v3::LabelId;
using PropertyId = memgraph::storage::v3::PropertyId;
using memgraph::coordinator::Shards;
using CompoundKey = memgraph::coordinator::PrimaryKey;
using memgraph::expr::ExpressionRuntimeException;
using memgraph::functions::FunctionRuntimeException;

namespace {

class MockedShardRequestManager : public memgraph::msgs::ShardRequestManagerInterface {
 public:
  using VertexAccessor = memgraph::query::v2::accessors::VertexAccessor;
  explicit MockedShardRequestManager(ShardMap shard_map) : shards_map_(std::move(shard_map)) { SetUpNameIdMappers(); }
  memgraph::storage::v3::EdgeTypeId NameToEdgeType(const std::string &name) const override {
    return shards_map_.GetEdgeTypeId(name).value();
  }

  memgraph::storage::v3::PropertyId NameToProperty(const std::string &name) const override {
    return shards_map_.GetPropertyId(name).value();
  }

  memgraph::storage::v3::LabelId NameToLabel(const std::string &name) const override {
    return shards_map_.GetLabelId(name).value();
  }
  void StartTransaction() override {}
  void Commit() override {}
  std::vector<VertexAccessor> Request(
      memgraph::msgs::ExecutionState<memgraph::msgs::ScanVerticesRequest> &state) override {
    return {};
  }

  std::vector<CreateVerticesResponse> Request(memgraph::msgs::ExecutionState<CreateVerticesRequest> &state,
                                              std::vector<memgraph::msgs::NewVertex> new_vertices) override {
    return {};
  }

  std::vector<ExpandOneResultRow> Request(memgraph::msgs::ExecutionState<ExpandOneRequest> &state,
                                          ExpandOneRequest request) override {
    return {};
  }

  std::vector<CreateExpandResponse> Request(memgraph::msgs::ExecutionState<CreateExpandRequest> &state,
                                            std::vector<NewExpand> new_edges) override {
    return {};
  }

  const std::string &PropertyToName(memgraph::storage::v3::PropertyId id) const override {
    return properties_.IdToName(id.AsUint());
  }

  const std::string &LabelToName(memgraph::storage::v3::LabelId id) const override {
    return labels_.IdToName(id.AsUint());
  }

  const std::string &EdgeTypeToName(memgraph::storage::v3::EdgeTypeId id) const override {
    return edge_types_.IdToName(id.AsUint());
  }

  bool IsPrimaryLabel(LabelId label) const override { return true; }

  bool IsPrimaryKey(LabelId primary_label, PropertyId property) const override { return true; }

 private:
  void SetUpNameIdMappers() {
    std::unordered_map<uint64_t, std::string> id_to_name;
    for (const auto &[name, id] : shards_map_.labels) {
      id_to_name.emplace(id.AsUint(), name);
    }
    labels_.StoreMapping(std::move(id_to_name));
    id_to_name.clear();
    for (const auto &[name, id] : shards_map_.properties) {
      id_to_name.emplace(id.AsUint(), name);
    }
    properties_.StoreMapping(std::move(id_to_name));
    id_to_name.clear();
    for (const auto &[name, id] : shards_map_.edge_types) {
      id_to_name.emplace(id.AsUint(), name);
    }
    edge_types_.StoreMapping(std::move(id_to_name));
  }

  ShardMap shards_map_;
  memgraph::storage::v3::NameIdMapper properties_;
  memgraph::storage::v3::NameIdMapper edge_types_;
  memgraph::storage::v3::NameIdMapper labels_;
};

ShardMap CreateDummyShardmap() {
  static const std::string label_name = std::string("label1");
  ShardMap sm;

  // register new properties
  const std::vector<std::string> property_names = {"prop", "property_2", "age", "height", "a", "b", "c"};
  const auto properties = sm.AllocatePropertyIds(property_names);
  const auto property_id_1 = properties.at("prop");
  const auto property_id_2 = properties.at("property_2");
  const auto property_id_3 = properties.at("age");
  const auto property_id_4 = properties.at("height");
  const auto property_id_5 = properties.at("a");
  const auto property_id_6 = properties.at("b");
  const auto property_id_7 = properties.at("c");
  const auto type_1 = memgraph::common::SchemaType::INT;

  using SchemaProperty = memgraph::coordinator::SchemaProperty;
  // register new label space
  std::vector<SchemaProperty> schema = {
      SchemaProperty{.property_id = property_id_1, .type = type_1},
      SchemaProperty{.property_id = property_id_2, .type = type_1},
      SchemaProperty{.property_id = property_id_3, .type = type_1},
      SchemaProperty{.property_id = property_id_4, .type = type_1},
      SchemaProperty{.property_id = property_id_5, .type = type_1},
      SchemaProperty{.property_id = property_id_6, .type = type_1},
      SchemaProperty{.property_id = property_id_7, .type = type_1},
  };

  auto label_success = sm.InitializeNewLabel(label_name, schema, 1, sm.shard_map_version);
  MG_ASSERT(label_success);

  const LabelId label_id = sm.labels.at(label_name);
  auto &label_space = sm.label_spaces.at(label_id);
  Shards &shards_for_label = label_space.shards;
  shards_for_label.clear();

  auto key1 = memgraph::storage::v3::PropertyValue(0);
  auto key2 = memgraph::storage::v3::PropertyValue(0);
  CompoundKey compound_key_1 = {key1, key2};
  shards_for_label[compound_key_1] = {};

  auto key3 = memgraph::storage::v3::PropertyValue(12);
  auto key4 = memgraph::storage::v3::PropertyValue(13);
  CompoundKey compound_key_2 = {key3, key4};
  shards_for_label[compound_key_2] = {};

  sm.AllocateEdgeTypeIds(std::vector<memgraph::coordinator::EdgeTypeName>{"edge_type"});

  return sm;
}

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  ExpressionEvaluatorTest() {}

 protected:
  //  memgraph::storage::Storage db;
  //  memgraph::storage::Storage::Accessor storage_dba{db.Access()};
  //  memgraph::query::DbAccessor dba{&storage_dba};
  //
  AstStorage storage;
  memgraph::utils::MonotonicBufferResource mem{1024};
  EvaluationContext ctx{.memory = &mem, .timestamp = memgraph::query::v2::QueryTimestamp()};
  SymbolTable symbol_table;

  Frame frame{128};
  std::unique_ptr<memgraph::msgs::ShardRequestManagerInterface> shard_manager =
      std::make_unique<MockedShardRequestManager>(CreateDummyShardmap());
  ExpressionEvaluator eval{&frame, symbol_table, ctx, shard_manager.get(), memgraph::storage::v3::View::OLD};

  Identifier *CreateIdentifierWithValue(std::string name, const TypedValue &value) {
    auto id = storage.Create<Identifier>(name, true);
    auto symbol = symbol_table.CreateSymbol(name, true);
    id->MapTo(symbol);
    frame[symbol] = value;
    return id;
  }

  template <class TExpression>
  auto Eval(TExpression *expr) {
    ctx.properties = NamesToProperties(storage.properties_, shard_manager.get());
    ctx.labels = NamesToLabels(storage.labels_, shard_manager.get());
    auto value = expr->Accept(eval);
    EXPECT_EQ(value.GetMemoryResource(), &mem) << "ExpressionEvaluator must use the MemoryResource from "
                                                  "EvaluationContext for allocations!";
    return value;
  }
};

TEST_F(ExpressionEvaluatorTest, OrOperator) {
  auto *op =
      storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(false));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(true));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
}

TEST_F(ExpressionEvaluatorTest, XorOperator) {
  auto *op =
      storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(false));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(true));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
}

TEST_F(ExpressionEvaluatorTest, AndOperator) {
  auto *op =
      storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(true), storage.Create<PrimitiveLiteral>(true));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(false), storage.Create<PrimitiveLiteral>(true));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
}

TEST_F(ExpressionEvaluatorTest, AndOperatorShortCircuit) {
  {
    auto *op =
        storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(false), storage.Create<PrimitiveLiteral>(5));
    auto value = Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    auto *op =
        storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(5), storage.Create<PrimitiveLiteral>(false));
    // We are evaluating left to right, so we don't short circuit here and
    // raise due to `5`. This differs from neo4j, where they evaluate both
    // sides and return `false` without checking for type of the first
    // expression.
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
}

TEST_F(ExpressionEvaluatorTest, AndOperatorNull) {
  {
    // Null doesn't short circuit
    auto *op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                           storage.Create<PrimitiveLiteral>(5));
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
  {
    auto *op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                           storage.Create<PrimitiveLiteral>(true));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    auto *op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                           storage.Create<PrimitiveLiteral>(false));
    auto value = Eval(op);
    ASSERT_TRUE(value.IsBool());
    EXPECT_EQ(value.ValueBool(), false);
  }
}

TEST_F(ExpressionEvaluatorTest, AdditionOperator) {
  auto *op = storage.Create<AdditionOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TEST_F(ExpressionEvaluatorTest, SubtractionOperator) {
  auto *op =
      storage.Create<SubtractionOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), -1);
}

TEST_F(ExpressionEvaluatorTest, MultiplicationOperator) {
  auto *op =
      storage.Create<MultiplicationOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), 6);
}

TEST_F(ExpressionEvaluatorTest, DivisionOperator) {
  auto *op =
      storage.Create<DivisionOperator>(storage.Create<PrimitiveLiteral>(50), storage.Create<PrimitiveLiteral>(10));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TEST_F(ExpressionEvaluatorTest, ModOperator) {
  auto *op = storage.Create<ModOperator>(storage.Create<PrimitiveLiteral>(65), storage.Create<PrimitiveLiteral>(10));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TEST_F(ExpressionEvaluatorTest, EqualOperator) {
  auto *op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TEST_F(ExpressionEvaluatorTest, NotEqualOperator) {
  auto *op =
      storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TEST_F(ExpressionEvaluatorTest, LessOperator) {
  auto *op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TEST_F(ExpressionEvaluatorTest, GreaterOperator) {
  auto *op =
      storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TEST_F(ExpressionEvaluatorTest, LessEqualOperator) {
  auto *op =
      storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TEST_F(ExpressionEvaluatorTest, GreaterEqualOperator) {
  auto *op =
      storage.Create<GreaterEqualOperator>(storage.Create<PrimitiveLiteral>(10), storage.Create<PrimitiveLiteral>(15));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = storage.Create<GreaterEqualOperator>(storage.Create<PrimitiveLiteral>(15), storage.Create<PrimitiveLiteral>(15));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = storage.Create<GreaterEqualOperator>(storage.Create<PrimitiveLiteral>(20), storage.Create<PrimitiveLiteral>(15));
  auto val3 = Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TEST_F(ExpressionEvaluatorTest, InListOperator) {
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
      storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>("a")});
  {
    // Element exists in list.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>(2), list_literal);
    auto value = Eval(op);
    EXPECT_EQ(value.ValueBool(), true);
  }
  {
    // Element doesn't exist in list.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>("x"), list_literal);
    auto value = Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    auto *list_literal = storage.Create<ListLiteral>(
        std::vector<Expression *>{storage.Create<PrimitiveLiteral>(TypedValue()), storage.Create<PrimitiveLiteral>(2),
                                  storage.Create<PrimitiveLiteral>("a")});
    // Element doesn't exist in list with null element.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>("x"), list_literal);
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null list.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>("x"),
                                              storage.Create<PrimitiveLiteral>(TypedValue()));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>(TypedValue()), list_literal);
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal, empty list.
    auto *op = storage.Create<InListOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                              storage.Create<ListLiteral>(std::vector<Expression *>()));
    auto value = Eval(op);
    EXPECT_FALSE(value.ValueBool());
  }
}

TEST_F(ExpressionEvaluatorTest, ListIndexing) {
  auto *list_literal = storage.Create<ListLiteral>(
      std::vector<Expression *>{storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
                                storage.Create<PrimitiveLiteral>(3), storage.Create<PrimitiveLiteral>(4)});
  {
    // Legal indexing.
    auto *op = storage.Create<SubscriptOperator>(list_literal, storage.Create<PrimitiveLiteral>(2));
    auto value = Eval(op);
    EXPECT_EQ(value.ValueInt(), 3);
  }
  {
    // Out of bounds indexing.
    auto *op = storage.Create<SubscriptOperator>(list_literal, storage.Create<PrimitiveLiteral>(4));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Out of bounds indexing with negative bound.
    auto *op = storage.Create<SubscriptOperator>(list_literal, storage.Create<PrimitiveLiteral>(-100));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Legal indexing with negative index.
    auto *op = storage.Create<SubscriptOperator>(list_literal, storage.Create<PrimitiveLiteral>(-2));
    auto value = Eval(op);
    EXPECT_EQ(value.ValueInt(), 3);
  }
  {
    // Indexing with one operator being null.
    auto *op = storage.Create<SubscriptOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                                 storage.Create<PrimitiveLiteral>(-2));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Indexing with incompatible type.
    auto *op = storage.Create<SubscriptOperator>(list_literal, storage.Create<PrimitiveLiteral>("bla"));
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
}

TEST_F(ExpressionEvaluatorTest, MapIndexing) {
  auto *map_literal = storage.Create<MapLiteral>(
      std::unordered_map<PropertyIx, Expression *>{{storage.GetPropertyIx("a"), storage.Create<PrimitiveLiteral>(1)},
                                                   {storage.GetPropertyIx("b"), storage.Create<PrimitiveLiteral>(2)},
                                                   {storage.GetPropertyIx("c"), storage.Create<PrimitiveLiteral>(3)}});
  {
    // Legal indexing.
    auto *op = storage.Create<SubscriptOperator>(map_literal, storage.Create<PrimitiveLiteral>("b"));
    auto value = Eval(op);
    EXPECT_EQ(value.ValueInt(), 2);
  }
  {
    // Legal indexing, non-existing key.
    auto *op = storage.Create<SubscriptOperator>(map_literal, storage.Create<PrimitiveLiteral>("z"));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Wrong key type.
    auto *op = storage.Create<SubscriptOperator>(map_literal, storage.Create<PrimitiveLiteral>(42));
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
  {
    // Indexing with Null.
    auto *op = storage.Create<SubscriptOperator>(map_literal, storage.Create<PrimitiveLiteral>(TypedValue()));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
}

using Vertex = memgraph::msgs::Vertex;
using Edge = memgraph::msgs::Edge;
using EdgeType = memgraph::msgs::EdgeType;
using Value = memgraph::msgs::Value;
using VertexId = memgraph::msgs::VertexId;
using Label = memgraph::msgs::Label;

memgraph::query::v2::accessors::VertexAccessor CreateVertex(std::vector<std::pair<PropertyId, Value>> props,
                                                            const memgraph::msgs::ShardRequestManagerInterface *manager,
                                                            Label label = {}) {
  static size_t id = 0;
  return {Vertex{VertexId{label, id++}, {label}}, std::move(props), manager};
}

memgraph::query::v2::accessors::EdgeAccessor CreateEdge(std::vector<std::pair<PropertyId, Value>> props,
                                                        const memgraph::msgs::ShardRequestManagerInterface *manager) {
  auto edge = Edge{.src = VertexId{{}, 0},
                   .dst = VertexId{{}, 0},
                   .properties = std::move(props),
                   .type = EdgeType{manager->NameToEdgeType("edge_type")}};
  return memgraph::query::v2::accessors::EdgeAccessor{std::move(edge), manager};
}

TEST_F(ExpressionEvaluatorTest, VertexAndEdgeIndexing) {
  auto prop = shard_manager->NameToProperty("prop");
  auto vertex = CreateVertex({{prop, Value(static_cast<int64_t>(42))}}, shard_manager.get());
  auto edge = CreateEdge({{prop, Value(static_cast<int64_t>(43))}}, shard_manager.get());

  auto *vertex_id = CreateIdentifierWithValue("v1", TypedValue(vertex));
  auto *edge_id = CreateIdentifierWithValue("e11", TypedValue(edge));
  {
    // Legal indexing.
    auto *op1 = storage.Create<SubscriptOperator>(vertex_id, storage.Create<PrimitiveLiteral>("prop"));
    auto value1 = Eval(op1);
    EXPECT_EQ(value1.ValueInt(), 42);

    auto *op2 = storage.Create<SubscriptOperator>(edge_id, storage.Create<PrimitiveLiteral>("prop"));
    auto value2 = Eval(op2);
    EXPECT_EQ(value2.ValueInt(), 43);
  }
  // TODO(kostasrim) Investigate
  //  Shall we return null on missing properties? Or shall we throw bad optional access as we do now?

  //   {
  //     // Legal indexing, non-existing key.
  //     auto *op1 = storage.Create<SubscriptOperator>(vertex_id, storage.Create<PrimitiveLiteral>("blah"));
  //     auto value1 = Eval(op1);
  //     EXPECT_TRUE(value1.IsNull());
  //
  //     auto *op2 = storage.Create<SubscriptOperator>(edge_id, storage.Create<PrimitiveLiteral>("blah"));
  //     auto value2 = Eval(op2);
  //     EXPECT_TRUE(value2.IsNull());
  //   }
  //   {
  //     // Wrong key type.
  //     auto *op1 = storage.Create<SubscriptOperator>(vertex_id, storage.Create<PrimitiveLiteral>(1));
  //     EXPECT_THROW(Eval(op1), ExpressionRuntimeException);
  //
  //     auto *op2 = storage.Create<SubscriptOperator>(edge_id, storage.Create<PrimitiveLiteral>(1));
  //     EXPECT_THROW(Eval(op2), ExpressionRuntimeException);
  //   }
  //   {
  //     // Indexing with Null.
  //     auto *op1 = storage.Create<SubscriptOperator>(vertex_id,
  //                                                   storage.Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
  //     auto value1 = Eval(op1);
  //     EXPECT_TRUE(value1.IsNull());
  //
  //     auto *op2 = storage.Create<SubscriptOperator>(edge_id,
  //                                                   storage.Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
  //     auto value2 = Eval(op2);
  //     EXPECT_TRUE(value2.IsNull());
  //   }
}

TEST_F(ExpressionEvaluatorTest, ListSlicingOperator) {
  auto *list_literal = storage.Create<ListLiteral>(
      std::vector<Expression *>{storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
                                storage.Create<PrimitiveLiteral>(3), storage.Create<PrimitiveLiteral>(4)});

  auto extract_ints = [](TypedValue list) {
    std::vector<int64_t> int_list;
    for (auto x : list.ValueList()) {
      int_list.push_back(x.ValueInt());
    }
    return int_list;
  };
  {
    // Legal slicing with both bounds defined.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(2),
                                                   storage.Create<PrimitiveLiteral>(4));
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Legal slicing with negative bound.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(2),
                                                   storage.Create<PrimitiveLiteral>(-1));
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3));
  }
  {
    // Lower bound larger than upper bound.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(2),
                                                   storage.Create<PrimitiveLiteral>(-4));
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre());
  }
  {
    // Bounds ouf or range.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(-100),
                                                   storage.Create<PrimitiveLiteral>(10));
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3, 4));
  }
  {
    // Lower bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, nullptr, storage.Create<PrimitiveLiteral>(3));
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3));
  }
  {
    // Upper bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(-2), nullptr);
    auto value = Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Bound of illegal type and null value bound.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(TypedValue()),
                                                   storage.Create<PrimitiveLiteral>("mirko"));
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
  {
    // List of illegal type.
    auto *op = storage.Create<ListSlicingOperator>(storage.Create<PrimitiveLiteral>("a"),
                                                   storage.Create<PrimitiveLiteral>(-2), nullptr);
    EXPECT_THROW(Eval(op), ExpressionRuntimeException);
  }
  {
    // Null value list with undefined upper bound.
    auto *op = storage.Create<ListSlicingOperator>(storage.Create<PrimitiveLiteral>(TypedValue()),
                                                   storage.Create<PrimitiveLiteral>(-2), nullptr);
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
    ;
  }
  {
    // Null value index.
    auto *op = storage.Create<ListSlicingOperator>(list_literal, storage.Create<PrimitiveLiteral>(-2),
                                                   storage.Create<PrimitiveLiteral>(TypedValue()));
    auto value = Eval(op);
    EXPECT_TRUE(value.IsNull());
    ;
  }
}

TEST_F(ExpressionEvaluatorTest, IfOperator) {
  auto *then_expression = storage.Create<PrimitiveLiteral>(10);
  auto *else_expression = storage.Create<PrimitiveLiteral>(20);
  {
    auto *condition_true =
        storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(2));
    auto *op = storage.Create<IfOperator>(condition_true, then_expression, else_expression);
    auto value = Eval(op);
    ASSERT_EQ(value.ValueInt(), 10);
  }
  {
    auto *condition_false =
        storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
    auto *op = storage.Create<IfOperator>(condition_false, then_expression, else_expression);
    auto value = Eval(op);
    ASSERT_EQ(value.ValueInt(), 20);
  }
  {
    auto *condition_exception =
        storage.Create<AdditionOperator>(storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
    auto *op = storage.Create<IfOperator>(condition_exception, then_expression, else_expression);
    ASSERT_THROW(Eval(op), ExpressionRuntimeException);
  }
}

TEST_F(ExpressionEvaluatorTest, NotOperator) {
  auto *op = storage.Create<NotOperator>(storage.Create<PrimitiveLiteral>(false));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueBool(), true);
}

TEST_F(ExpressionEvaluatorTest, UnaryPlusOperator) {
  auto *op = storage.Create<UnaryPlusOperator>(storage.Create<PrimitiveLiteral>(5));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TEST_F(ExpressionEvaluatorTest, UnaryMinusOperator) {
  auto *op = storage.Create<UnaryMinusOperator>(storage.Create<PrimitiveLiteral>(5));
  auto value = Eval(op);
  ASSERT_EQ(value.ValueInt(), -5);
}

TEST_F(ExpressionEvaluatorTest, IsNullOperator) {
  auto *op = storage.Create<IsNullOperator>(storage.Create<PrimitiveLiteral>(1));
  auto val1 = Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = storage.Create<IsNullOperator>(storage.Create<PrimitiveLiteral>(TypedValue()));
  auto val2 = Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
}

// TEST_F(ExpressionEvaluatorTest, LabelsTest) {
//   auto v1 = dba.InsertVertex();
//   ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("ANIMAL")).HasValue());
//   ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("DOG")).HasValue());
//   ASSERT_TRUE(v1.AddLabel(dba.NameToLabel("NICE_DOG")).HasValue());
//   dba.AdvanceCommand();
//   auto *identifier = storage.Create<Identifier>("n");
//   auto node_symbol = symbol_table.CreateSymbol("n", true);
//   identifier->MapTo(node_symbol);
//   frame[node_symbol] = TypedValue(v1);
//   {
//     auto *op = storage.Create<LabelsTest>(
//         identifier, std::vector<LabelIx>{storage.GetLabelIx("DOG"), storage.GetLabelIx("ANIMAL")});
//     auto value = Eval(op);
//     EXPECT_EQ(value.ValueBool(), true);
//   }
//   {
//     auto *op = storage.Create<LabelsTest>(
//         identifier,
//         std::vector<LabelIx>{storage.GetLabelIx("DOG"), storage.GetLabelIx("BAD_DOG"),
//         storage.GetLabelIx("ANIMAL")});
//     auto value = Eval(op);
//     EXPECT_EQ(value.ValueBool(), false);
//   }
//   {
//     frame[node_symbol] = TypedValue();
//     auto *op = storage.Create<LabelsTest>(
//         identifier,
//         std::vector<LabelIx>{storage.GetLabelIx("DOG"), storage.GetLabelIx("BAD_DOG"),
//         storage.GetLabelIx("ANIMAL")});
//     auto value = Eval(op);
//     EXPECT_TRUE(value.IsNull());
//   }
// }
//
TEST_F(ExpressionEvaluatorTest, Aggregation) {
  auto aggr = storage.Create<Aggregation>(storage.Create<PrimitiveLiteral>(42), nullptr, Aggregation::Op::COUNT);
  auto aggr_sym = symbol_table.CreateSymbol("aggr", true);
  aggr->MapTo(aggr_sym);
  frame[aggr_sym] = TypedValue(1);
  auto value = Eval(aggr);
  EXPECT_EQ(value.ValueInt(), 1);
}

TEST_F(ExpressionEvaluatorTest, ListLiteral) {
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{storage.Create<PrimitiveLiteral>(1),
                                                                             storage.Create<PrimitiveLiteral>("bla"),
                                                                             storage.Create<PrimitiveLiteral>(true)});
  TypedValue result = Eval(list_literal);
  ASSERT_TRUE(result.IsList());
  auto &result_elems = result.ValueList();
  ASSERT_EQ(3, result_elems.size());
  EXPECT_TRUE(result_elems[0].IsInt());
  ;
  EXPECT_TRUE(result_elems[1].IsString());
  ;
  EXPECT_TRUE(result_elems[2].IsBool());
  ;
}

TEST_F(ExpressionEvaluatorTest, ParameterLookup) {
  ctx.parameters.Add(0, memgraph::storage::v3::PropertyValue(42));
  auto *param_lookup = storage.Create<ParameterLookup>(0);
  auto value = Eval(param_lookup);
  ASSERT_TRUE(value.IsInt());
  EXPECT_EQ(value.ValueInt(), 42);
}

TEST_F(ExpressionEvaluatorTest, FunctionAll1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(1)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAll2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAllNullList) {
  AstStorage storage;
  auto *all = ALL("x", LITERAL(TypedValue()), WHERE(LITERAL(true)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  auto value = Eval(all);
  EXPECT_TRUE(value.IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionAllNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAllNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(2), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAllWhereWrongType) {
  AstStorage storage;
  auto *all = ALL("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  EXPECT_THROW(Eval(all), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, FunctionSingle1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionSingle2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(GREATER(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionSingleNullList) {
  AstStorage storage;
  auto *single = SINGLE("x", LITERAL(TypedValue()), WHERE(LITERAL(true)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  auto value = Eval(single);
  EXPECT_TRUE(value.IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionSingleNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(1), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionSingleNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(2), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAny1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(any);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAny2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(any);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAnyNullList) {
  AstStorage storage;
  auto *any = ANY("x", LITERAL(TypedValue()), WHERE(LITERAL(true)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  auto value = Eval(any);
  EXPECT_TRUE(value.IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionAnyNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(0), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(any);
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAnyNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(any);
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionAnyWhereWrongType) {
  AstStorage storage;
  auto *any = ANY("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  EXPECT_THROW(Eval(any), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, FunctionNone1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(none);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionNone2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(none);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionNoneNullList) {
  AstStorage storage;
  auto *none = NONE("x", LITERAL(TypedValue()), WHERE(LITERAL(true)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  auto value = Eval(none);
  EXPECT_TRUE(value.IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionNoneNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = NONE("x", LIST(LITERAL(1), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(any);
  EXPECT_TRUE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionNoneNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(0), LITERAL(TypedValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(none);
  EXPECT_FALSE(value.ValueBool());
}

TEST_F(ExpressionEvaluatorTest, FunctionNoneWhereWrongType) {
  AstStorage storage;
  auto *none = NONE("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  EXPECT_THROW(Eval(none), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, FunctionReduce) {
  AstStorage storage;
  auto *ident_sum = IDENT("sum");
  auto *ident_x = IDENT("x");
  auto *reduce = REDUCE("sum", LITERAL(0), "x", LIST(LITERAL(1), LITERAL(2)), ADD(ident_sum, ident_x));
  const auto sum_sym = symbol_table.CreateSymbol("sum", true);
  reduce->accumulator_->MapTo(sum_sym);
  ident_sum->MapTo(sum_sym);
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  reduce->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(reduce);
  ASSERT_TRUE(value.IsInt());
  EXPECT_EQ(value.ValueInt(), 3);
}

TEST_F(ExpressionEvaluatorTest, FunctionExtract) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract = EXTRACT("x", LIST(LITERAL(1), LITERAL(2), LITERAL(TypedValue())), ADD(ident_x, LITERAL(1)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(extract);
  EXPECT_TRUE(value.IsList());
  ;
  auto result = value.ValueList();
  EXPECT_EQ(result[0].ValueInt(), 2);
  EXPECT_EQ(result[1].ValueInt(), 3);
  EXPECT_TRUE(result[2].IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionExtractNull) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract = EXTRACT("x", LITERAL(TypedValue()), ADD(ident_x, LITERAL(1)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = Eval(extract);
  EXPECT_TRUE(value.IsNull());
}

TEST_F(ExpressionEvaluatorTest, FunctionExtractExceptions) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract = EXTRACT("x", LITERAL("bla"), ADD(ident_x, LITERAL(1)));
  const auto x_sym = symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  EXPECT_THROW(Eval(extract), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, RegexMatchInvalidArguments) {
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LITERAL(TypedValue()), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LITERAL(3), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LIST(LITERAL("string")), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LITERAL("string"), LITERAL(TypedValue()))).IsNull());
  EXPECT_THROW(Eval(storage.Create<RegexMatch>(LITERAL("string"), LITERAL(42))), ExpressionRuntimeException);
  EXPECT_THROW(Eval(storage.Create<RegexMatch>(LITERAL("string"), LIST(LITERAL("regex")))), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, RegexMatchInvalidRegex) {
  EXPECT_THROW(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL("*ext"))), ExpressionRuntimeException);
  EXPECT_THROW(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL("[ext"))), ExpressionRuntimeException);
}

TEST_F(ExpressionEvaluatorTest, RegexMatch) {
  EXPECT_FALSE(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL(".*ex"))).ValueBool());
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL(".*ext"))).ValueBool());
  EXPECT_FALSE(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL("[ext]"))).ValueBool());
  EXPECT_TRUE(Eval(storage.Create<RegexMatch>(LITERAL("text"), LITERAL(".+[ext]"))).ValueBool());
}

class ExpressionEvaluatorPropertyLookup : public ExpressionEvaluatorTest {
 protected:
  std::pair<std::string, memgraph::storage::v3::PropertyId> prop_age =
      std::make_pair("age", shard_manager->NameToProperty("age"));
  std::pair<std::string, memgraph::storage::v3::PropertyId> prop_height =
      std::make_pair("height", shard_manager->NameToProperty("height"));
  Identifier *identifier = storage.Create<Identifier>("element");
  Symbol symbol = symbol_table.CreateSymbol("element", true);

  void SetUp() { identifier->MapTo(symbol); }

  auto Value(std::pair<std::string, memgraph::storage::v3::PropertyId> property) {
    auto *op = storage.Create<PropertyLookup>(identifier, storage.GetPropertyIx(property.first));
    return Eval(op);
  }
};

// TEST_F(ExpressionEvaluatorPropertyLookup, Vertex) {
//   auto v1 = dba.InsertVertex();
//   ASSERT_TRUE(v1.SetProperty(prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
//   dba.AdvanceCommand();
//   frame[symbol] = TypedValue(v1);
//   EXPECT_EQ(Value(prop_age).ValueInt(), 10);
//   EXPECT_TRUE(Value(prop_height).IsNull());
// }
//
// TEST_F(ExpressionEvaluatorPropertyLookup, Duration) {
//   const memgraph::utils::Duration dur({10, 1, 30, 2, 22, 45});
//   frame[symbol] = TypedValue(dur);
//
//   const std::pair day = std::make_pair("day", dba.NameToProperty("day"));
//   const auto total_days = Value(day);
//   EXPECT_TRUE(total_days.IsInt());
//   EXPECT_EQ(total_days.ValueInt(), 10);
//
//   const std::pair hour = std::make_pair("hour", dba.NameToProperty("hour"));
//   const auto total_hours = Value(hour);
//   EXPECT_TRUE(total_hours.IsInt());
//   EXPECT_EQ(total_hours.ValueInt(), 1);
//
//   const std::pair minute = std::make_pair("minute", dba.NameToProperty("minute"));
//   const auto total_mins = Value(minute);
//   EXPECT_TRUE(total_mins.IsInt());
//
//   EXPECT_EQ(total_mins.ValueInt(), 1 * 60 + 30);
//
//   const std::pair sec = std::make_pair("second", dba.NameToProperty("second"));
//   const auto total_secs = Value(sec);
//   EXPECT_TRUE(total_secs.IsInt());
//   const auto expected_secs = total_mins.ValueInt() * 60 + 2;
//   EXPECT_EQ(total_secs.ValueInt(), expected_secs);
//
//   const std::pair milli = std::make_pair("millisecond", dba.NameToProperty("millisecond"));
//   const auto total_milli = Value(milli);
//   EXPECT_TRUE(total_milli.IsInt());
//   const auto expected_milli = total_secs.ValueInt() * 1000 + 22;
//   EXPECT_EQ(total_milli.ValueInt(), expected_milli);
//
//   const std::pair micro = std::make_pair("microsecond", dba.NameToProperty("microsecond"));
//   const auto total_micros = Value(micro);
//   EXPECT_TRUE(total_micros.IsInt());
//   const auto expected_micros = expected_milli * 1000 + 45;
//   EXPECT_EQ(total_micros.ValueInt(), expected_micros);
//
//   const std::pair nano = std::make_pair("nanosecond", dba.NameToProperty("nanosecond"));
//   const auto total_nano = Value(nano);
//   EXPECT_TRUE(total_nano.IsInt());
//   const auto expected_nano = expected_micros * 1000;
//   EXPECT_EQ(total_nano.ValueInt(), expected_nano);
// }
//
// TEST_F(ExpressionEvaluatorPropertyLookup, Date) {
//   const memgraph::utils::Date date({1996, 11, 22});
//   frame[symbol] = TypedValue(date);
//
//   const std::pair year = std::make_pair("year", dba.NameToProperty("year"));
//   const auto y = Value(year);
//   EXPECT_TRUE(y.IsInt());
//   EXPECT_EQ(y.ValueInt(), 1996);
//
//   const std::pair month = std::make_pair("month", dba.NameToProperty("month"));
//   const auto m = Value(month);
//   EXPECT_TRUE(m.IsInt());
//   EXPECT_EQ(m.ValueInt(), 11);
//
//   const std::pair day = std::make_pair("day", dba.NameToProperty("day"));
//   const auto d = Value(day);
//   EXPECT_TRUE(d.IsInt());
//   EXPECT_EQ(d.ValueInt(), 22);
// }
//
// TEST_F(ExpressionEvaluatorPropertyLookup, LocalTime) {
//   const memgraph::utils::LocalTime lt({1, 2, 3, 11, 22});
//   frame[symbol] = TypedValue(lt);
//
//   const std::pair hour = std::make_pair("hour", dba.NameToProperty("hour"));
//   const auto h = Value(hour);
//   EXPECT_TRUE(h.IsInt());
//   EXPECT_EQ(h.ValueInt(), 1);
//
//   const std::pair minute = std::make_pair("minute", dba.NameToProperty("minute"));
//   const auto min = Value(minute);
//   EXPECT_TRUE(min.IsInt());
//   EXPECT_EQ(min.ValueInt(), 2);
//
//   const std::pair second = std::make_pair("second", dba.NameToProperty("second"));
//   const auto sec = Value(second);
//   EXPECT_TRUE(sec.IsInt());
//   EXPECT_EQ(sec.ValueInt(), 3);
//
//   const std::pair millis = std::make_pair("millisecond", dba.NameToProperty("millisecond"));
//   const auto mil = Value(millis);
//   EXPECT_TRUE(mil.IsInt());
//   EXPECT_EQ(mil.ValueInt(), 11);
//
//   const std::pair micros = std::make_pair("microsecond", dba.NameToProperty("microsecond"));
//   const auto mic = Value(micros);
//   EXPECT_TRUE(mic.IsInt());
//   EXPECT_EQ(mic.ValueInt(), 22);
// }
//
// TEST_F(ExpressionEvaluatorPropertyLookup, LocalDateTime) {
//   const memgraph::utils::LocalDateTime ldt({1993, 8, 6}, {2, 3, 4, 55, 40});
//   frame[symbol] = TypedValue(ldt);
//
//   const std::pair year = std::make_pair("year", dba.NameToProperty("year"));
//   const auto y = Value(year);
//   EXPECT_TRUE(y.IsInt());
//   EXPECT_EQ(y.ValueInt(), 1993);
//
//   const std::pair month = std::make_pair("month", dba.NameToProperty("month"));
//   const auto m = Value(month);
//   EXPECT_TRUE(m.IsInt());
//   EXPECT_EQ(m.ValueInt(), 8);
//
//   const std::pair day = std::make_pair("day", dba.NameToProperty("day"));
//   const auto d = Value(day);
//   EXPECT_TRUE(d.IsInt());
//   EXPECT_EQ(d.ValueInt(), 6);
//
//   const std::pair hour = std::make_pair("hour", dba.NameToProperty("hour"));
//   const auto h = Value(hour);
//   EXPECT_TRUE(h.IsInt());
//   EXPECT_EQ(h.ValueInt(), 2);
//
//   const std::pair minute = std::make_pair("minute", dba.NameToProperty("minute"));
//   const auto min = Value(minute);
//   EXPECT_TRUE(min.IsInt());
//   EXPECT_EQ(min.ValueInt(), 3);
//
//   const std::pair second = std::make_pair("second", dba.NameToProperty("second"));
//   const auto sec = Value(second);
//   EXPECT_TRUE(sec.IsInt());
//   EXPECT_EQ(sec.ValueInt(), 4);
//
//   const std::pair millis = std::make_pair("millisecond", dba.NameToProperty("millisecond"));
//   const auto mil = Value(millis);
//   EXPECT_TRUE(mil.IsInt());
//   EXPECT_EQ(mil.ValueInt(), 55);
//
//   const std::pair micros = std::make_pair("microsecond", dba.NameToProperty("microsecond"));
//   const auto mic = Value(micros);
//   EXPECT_TRUE(mic.IsInt());
//   EXPECT_EQ(mic.ValueInt(), 40);
// }
//
// TEST_F(ExpressionEvaluatorPropertyLookup, Edge) {
//   auto v1 = dba.InsertVertex();
//   auto v2 = dba.InsertVertex();
//   auto e12 = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("edge_type"));
//   ASSERT_TRUE(e12.HasValue());
//   ASSERT_TRUE(e12->SetProperty(prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
//   dba.AdvanceCommand();
//   frame[symbol] = TypedValue(*e12);
//   EXPECT_EQ(Value(prop_age).ValueInt(), 10);
//   EXPECT_TRUE(Value(prop_height).IsNull());
// }
//
TEST_F(ExpressionEvaluatorPropertyLookup, Null) {
  frame[symbol] = TypedValue();
  EXPECT_TRUE(Value(prop_age).IsNull());
}

TEST_F(ExpressionEvaluatorPropertyLookup, MapLiteral) {
  frame[symbol] = TypedValue(std::map<std::string, TypedValue>{{prop_age.first, TypedValue(10)}});
  EXPECT_EQ(Value(prop_age).ValueInt(), 10);
  EXPECT_TRUE(Value(prop_height).IsNull());
}

class FunctionTest : public ExpressionEvaluatorTest {
 protected:
  std::vector<Expression *> ExpressionsFromTypedValues(const std::vector<TypedValue> &tvs) {
    std::vector<Expression *> expressions;
    expressions.reserve(tvs.size());

    for (size_t i = 0; i < tvs.size(); ++i) {
      auto *ident = storage.Create<Identifier>("arg_" + std::to_string(i), true);
      auto sym = symbol_table.CreateSymbol("arg_" + std::to_string(i), true);
      ident->MapTo(sym);
      frame[sym] = tvs[i];
      expressions.push_back(ident);
    }

    return expressions;
  }

  TypedValue EvaluateFunctionWithExprs(const std::string &function_name, const std::vector<Expression *> &expressions) {
    auto *op = storage.Create<Function>(function_name, expressions);
    return Eval(op);
  }

  template <class... TArgs>
  TypedValue EvaluateFunction(const std::string &function_name, std::tuple<TArgs...> args) {
    std::vector<TypedValue> tv_args;
    tv_args.reserve(args.size());
    for (auto &arg : args) tv_args.emplace_back(std::move(arg));
    return EvaluateFunctionWithExprs(function_name, ExpressionsFromTypedValues(tv_args));
  }

  template <class... TArgs>
  TypedValue EvaluateFunction(const std::string &function_name, TArgs &&...args) {
    return EvaluateFunctionWithExprs(function_name,
                                     ExpressionsFromTypedValues(std::vector<TypedValue>{TypedValue(args)...}));
  }
};

template <class... TArgs>
static TypedValue MakeTypedValueList(TArgs &&...args) {
  return TypedValue(std::vector<TypedValue>{TypedValue(args)...});
}

TEST_F(FunctionTest, EndNode) {
  ASSERT_THROW(EvaluateFunction("ENDNODE"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("ENDNODE", TypedValue()).IsNull());
  auto v1 = CreateVertex({}, shard_manager.get());
  auto e = CreateEdge({}, shard_manager.get());
  const auto expected = VertexId{{}, 0};
  const auto res = EvaluateFunction("ENDNODE", e).ValueVertex().Id();
  ASSERT_EQ(res, expected);
  ASSERT_THROW(EvaluateFunction("ENDNODE", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, Head) {
  ASSERT_THROW(EvaluateFunction("HEAD"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("HEAD", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(EvaluateFunction("HEAD", argument).ValueInt(), 3);
  argument.ValueList().clear();
  ASSERT_TRUE(EvaluateFunction("HEAD", argument).IsNull());
  ASSERT_THROW(EvaluateFunction("HEAD", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, Properties) {
  ASSERT_THROW(EvaluateFunction("PROPERTIES"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("PROPERTIES", TypedValue()).IsNull());
  const auto height = shard_manager->NameToProperty("height");
  const auto age = shard_manager->NameToProperty("age");
  auto v1 = CreateVertex({{height, Value(static_cast<int64_t>(5))}, {age, Value(static_cast<int64_t>(10))}},
                         shard_manager.get());
  auto e = CreateEdge({{height, Value(static_cast<int64_t>(3))}, {age, Value(static_cast<int64_t>(15))}},
                      shard_manager.get());

  auto prop_values_to_int = [](TypedValue t) {
    std::unordered_map<std::string, int> properties;
    for (auto property : t.ValueMap()) {
      properties[std::string(property.first)] = property.second.ValueInt();
    }
    return properties;
  };

  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", v1)),
              UnorderedElementsAre(testing::Pair("height", 5), testing::Pair("age", 10)));
  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", e)),
              UnorderedElementsAre(testing::Pair("height", 3), testing::Pair("age", 15)));
  ASSERT_THROW(EvaluateFunction("PROPERTIES", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, Last) {
  ASSERT_THROW(EvaluateFunction("LAST"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("LAST", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(EvaluateFunction("LAST", argument).ValueInt(), 5);
  argument.ValueList().clear();
  ASSERT_TRUE(EvaluateFunction("LAST", argument).IsNull());
  ASSERT_THROW(EvaluateFunction("LAST", 5), FunctionRuntimeException);
}

TEST_F(FunctionTest, Size) {
  ASSERT_THROW(EvaluateFunction("SIZE"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("SIZE", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(EvaluateFunction("SIZE", argument).ValueInt(), 3);
  ASSERT_EQ(EvaluateFunction("SIZE", "john").ValueInt(), 4);
  ASSERT_EQ(EvaluateFunction("SIZE",
                             std::map<std::string, TypedValue>{
                                 {"a", TypedValue(5)}, {"b", TypedValue(true)}, {"c", TypedValue("123")}})
                .ValueInt(),
            3);
  ASSERT_THROW(EvaluateFunction("SIZE", 5), FunctionRuntimeException);

  // TODO(kostasrim) Add this when we enable paths on the accessors
  //  auto v0 = dba.InsertVertex();
  //  memgraph::query::Path path(v0);
  //  EXPECT_EQ(EvaluateFunction("SIZE", path).ValueInt(), 0);
  //  auto v1 = dba.InsertVertex();
  //  auto edge = dba.InsertEdge(&v0, &v1, dba.NameToEdgeType("type"));
  //  ASSERT_TRUE(edge.HasValue());
  //  path.Expand(*edge);
  //  path.Expand(v1);
  //  EXPECT_EQ(EvaluateFunction("SIZE", path).ValueInt(), 1);
}

TEST_F(FunctionTest, StartNode) {
  ASSERT_THROW(EvaluateFunction("STARTNODE"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("STARTNODE", TypedValue()).IsNull());
  auto v1 = CreateVertex({}, shard_manager.get());
  auto e = CreateEdge({}, shard_manager.get());
  const auto expected = VertexId{{}, 0};
  const auto res = EvaluateFunction("STARTNODE", e).ValueVertex().Id();
  ASSERT_EQ(res, expected);
  ASSERT_THROW(EvaluateFunction("STARTNODE", 2), FunctionRuntimeException);
}

// TODO(kostasrim) Enable this test once we add degree to the accessors
// TEST_F(FunctionTest, Degree) {
//   ASSERT_THROW(EvaluateFunction("DEGREE"), ExpressionRuntimeException);
//   ASSERT_TRUE(EvaluateFunction("DEGREE", TypedValue()).IsNull());
//   auto v1 = dba.InsertVertex();
//   auto v2 = dba.InsertVertex();
//   auto v3 = dba.InsertVertex();
//   auto e12 = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("t"));
//   ASSERT_TRUE(e12.HasValue());
//   ASSERT_TRUE(dba.InsertEdge(&v3, &v2, dba.NameToEdgeType("t")).HasValue());
//   dba.AdvanceCommand();
//   ASSERT_EQ(EvaluateFunction("DEGREE", v1).ValueInt(), 1);
//   ASSERT_EQ(EvaluateFunction("DEGREE", v2).ValueInt(), 2);
//   ASSERT_EQ(EvaluateFunction("DEGREE", v3).ValueInt(), 1);
//   ASSERT_THROW(EvaluateFunction("DEGREE", 2), ExpressionRuntimeException);
//   ASSERT_THROW(EvaluateFunction("DEGREE", *e12), ExpressionRuntimeException);
// }
//
// TODO(kostasrim) Enable this test once we add InDegree to the accessors
// TEST_F(FunctionTest, InDegree) {
//   ASSERT_THROW(EvaluateFunction("INDEGREE"), ExpressionRuntimeException);
//   ASSERT_TRUE(EvaluateFunction("INDEGREE", TypedValue()).IsNull());
//   auto v1 = dba.InsertVertex();
//   auto v2 = dba.InsertVertex();
//   auto v3 = dba.InsertVertex();
//   auto e12 = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("t"));
//   ASSERT_TRUE(e12.HasValue());
//   ASSERT_TRUE(dba.InsertEdge(&v3, &v2, dba.NameToEdgeType("t")).HasValue());
//   dba.AdvanceCommand();
//   ASSERT_EQ(EvaluateFunction("INDEGREE", v1).ValueInt(), 0);
//   ASSERT_EQ(EvaluateFunction("INDEGREE", v2).ValueInt(), 2);
//   ASSERT_EQ(EvaluateFunction("INDEGREE", v3).ValueInt(), 0);
//   ASSERT_THROW(EvaluateFunction("INDEGREE", 2), ExpressionRuntimeException);
//   ASSERT_THROW(EvaluateFunction("INDEGREE", *e12), ExpressionRuntimeException);
// }
//
// TODO(kostasrim) Enable this test once we add OutDegree to the accessors
// TEST_F(FunctionTest, OutDegree) {
//   ASSERT_THROW(EvaluateFunction("OUTDEGREE"), ExpressionRuntimeException);
//   ASSERT_TRUE(EvaluateFunction("OUTDEGREE", TypedValue()).IsNull());
//   auto v1 = dba.InsertVertex();
//   auto v2 = dba.InsertVertex();
//   auto v3 = dba.InsertVertex();
//   auto e12 = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("t"));
//   ASSERT_TRUE(e12.HasValue());
//   ASSERT_TRUE(dba.InsertEdge(&v3, &v2, dba.NameToEdgeType("t")).HasValue());
//   dba.AdvanceCommand();
//   ASSERT_EQ(EvaluateFunction("OUTDEGREE", v1).ValueInt(), 1);
//   ASSERT_EQ(EvaluateFunction("OUTDEGREE", v2).ValueInt(), 0);
//   ASSERT_EQ(EvaluateFunction("OUTDEGREE", v3).ValueInt(), 1);
//   ASSERT_THROW(EvaluateFunction("OUTDEGREE", 2), ExpressionRuntimeException);
//   ASSERT_THROW(EvaluateFunction("OUTDEGREE", *e12), ExpressionRuntimeException);
// }
//
TEST_F(FunctionTest, ToBoolean) {
  ASSERT_THROW(EvaluateFunction("TOBOOLEAN"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("TOBOOLEAN", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", 123).ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", -213).ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", 0).ValueBool(), false);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", " trUE \n\t").ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", "\n\tFalsE").ValueBool(), false);
  ASSERT_TRUE(EvaluateFunction("TOBOOLEAN", "\n\tFALSEA ").IsNull());
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", true).ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", false).ValueBool(), false);
}

TEST_F(FunctionTest, ToFloat) {
  ASSERT_THROW(EvaluateFunction("TOFLOAT"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("TOFLOAT", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("TOFLOAT", " -3.5 \n\t").ValueDouble(), -3.5);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", "\n\t0.5e-1").ValueDouble(), 0.05);
  ASSERT_TRUE(EvaluateFunction("TOFLOAT", "\n\t3.4e-3X ").IsNull());
  ASSERT_EQ(EvaluateFunction("TOFLOAT", -3.5).ValueDouble(), -3.5);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", -3).ValueDouble(), -3.0);
  ASSERT_THROW(EvaluateFunction("TOFLOAT", true), FunctionRuntimeException);
}

TEST_F(FunctionTest, ToInteger) {
  ASSERT_THROW(EvaluateFunction("TOINTEGER"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("TOINTEGER", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("TOINTEGER", false).ValueInt(), 0);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", true).ValueInt(), 1);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", "\n\t3").ValueInt(), 3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", " -3.5 \n\t").ValueInt(), -3);
  ASSERT_TRUE(EvaluateFunction("TOINTEGER", "\n\t3X ").IsNull());
  ASSERT_EQ(EvaluateFunction("TOINTEGER", -3.5).ValueInt(), -3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", 3.5).ValueInt(), 3);
}

TEST_F(FunctionTest, Type) {
  ASSERT_THROW(EvaluateFunction("TYPE"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("TYPE", TypedValue()).IsNull());
  auto e = CreateEdge({}, shard_manager.get());
  ASSERT_EQ(EvaluateFunction("TYPE", e).ValueString(), "edge_type");
  ASSERT_THROW(EvaluateFunction("TYPE", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, ValueType) {
  ASSERT_THROW(EvaluateFunction("VALUETYPE"), FunctionRuntimeException);
  ASSERT_THROW(EvaluateFunction("VALUETYPE", TypedValue(), TypedValue()), FunctionRuntimeException);
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue()).ValueString(), "NULL");
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue(true)).ValueString(), "BOOLEAN");
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue(1)).ValueString(), "INTEGER");
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue(1.1)).ValueString(), "FLOAT");
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue("test")).ValueString(), "STRING");
  ASSERT_EQ(
      EvaluateFunction("VALUETYPE", TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)})).ValueString(),
      "LIST");
  ASSERT_EQ(EvaluateFunction("VALUETYPE", TypedValue(std::map<std::string, TypedValue>{{"test", TypedValue(1)}}))
                .ValueString(),
            "MAP");
  auto v1 = CreateVertex({}, shard_manager.get());
  ASSERT_EQ(EvaluateFunction("VALUETYPE", v1).ValueString(), "NODE");
  auto e = CreateEdge({}, shard_manager.get());
  ASSERT_EQ(EvaluateFunction("VALUETYPE", e).ValueString(), "RELATIONSHIP");
  // TODO(kostasrim) Fix this when we add Path to accessors
  //  Path p(v1, *e, v2);
  //  ASSERT_EQ(EvaluateFunction("VALUETYPE", p).ValueString(), "PATH");
}

TEST_F(FunctionTest, Labels) {
  ASSERT_THROW(EvaluateFunction("LABELS"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("LABELS", TypedValue()).IsNull());
  Label label{shard_manager->NameToLabel("label1")};
  auto v = CreateVertex({}, shard_manager.get(), std::move(label));
  std::vector<std::string> labels;
  auto _labels = EvaluateFunction("LABELS", v).ValueList();
  labels.reserve(_labels.size());
  for (auto label : _labels) {
    labels.emplace_back(label.ValueString());
  }
  ASSERT_THAT(labels, UnorderedElementsAre("label1"));
  ASSERT_THROW(EvaluateFunction("LABELS", 2), FunctionRuntimeException);
}

// TODO(kostasrim) Enable this once we fix accessors Path
// TEST_F(FunctionTest, NodesRelationships) {
//   EXPECT_THROW(EvaluateFunction("NODES"), ExpressionRuntimeException);
//   EXPECT_THROW(EvaluateFunction("RELATIONSHIPS"), ExpressionRuntimeException);
//   EXPECT_TRUE(EvaluateFunction("NODES", TypedValue()).IsNull());
//   EXPECT_TRUE(EvaluateFunction("RELATIONSHIPS", TypedValue()).IsNull());
//
//   {
//     auto v1 = dba.InsertVertex();
//     auto v2 = dba.InsertVertex();
//     auto v3 = dba.InsertVertex();
//     auto e1 = dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("Type"));
//     ASSERT_TRUE(e1.HasValue());
//     auto e2 = dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("Type"));
//     ASSERT_TRUE(e2.HasValue());
//     memgraph::query::Path path{v1, *e1, v2, *e2, v3};
//     dba.AdvanceCommand();
//
//     auto _nodes = EvaluateFunction("NODES", path).ValueList();
//     std::vector<memgraph::query::VertexAccessor> nodes;
//     for (const auto &node : _nodes) {
//       nodes.push_back(node.ValueVertex());
//     }
//     EXPECT_THAT(nodes, ElementsAre(v1, v2, v3));
//
//     auto _edges = EvaluateFunction("RELATIONSHIPS", path).ValueList();
//     std::vector<memgraph::query::EdgeAccessor> edges;
//     for (const auto &edge : _edges) {
//       edges.push_back(edge.ValueEdge());
//     }
//     EXPECT_THAT(edges, ElementsAre(*e1, *e2));
//   }
//
//   EXPECT_THROW(EvaluateFunction("NODES", 2), ExpressionRuntimeException);
//   EXPECT_THROW(EvaluateFunction("RELATIONSHIPS", 2), ExpressionRuntimeException);
// }
//
TEST_F(FunctionTest, Range) {
  EXPECT_THROW(EvaluateFunction("RANGE"), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction("RANGE", 1, 2, TypedValue()).IsNull());
  EXPECT_THROW(EvaluateFunction("RANGE", 1, TypedValue(), 1.3), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("RANGE", 1, 2, 0), FunctionRuntimeException);
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 1, 3)), ElementsAre(1, 2, 3));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", -1, 5, 2)), ElementsAre(-1, 1, 3, 5));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 2, 10, 3)), ElementsAre(2, 5, 8));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 2, 2, 2)), ElementsAre(2));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 3, 0, 5)), ElementsAre());
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 5, 1, -2)), ElementsAre(5, 3, 1));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 6, 1, -2)), ElementsAre(6, 4, 2));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", 2, 2, -3)), ElementsAre(2));
  EXPECT_THAT(ToIntList(EvaluateFunction("RANGE", -2, 4, -1)), ElementsAre());
}

TEST_F(FunctionTest, Keys) {
  ASSERT_THROW(EvaluateFunction("KEYS"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("KEYS", TypedValue()).IsNull());
  const auto height = shard_manager->NameToProperty("height");
  const auto age = shard_manager->NameToProperty("age");
  auto v1 = CreateVertex({{height, Value(static_cast<int64_t>(5))}, {age, Value(static_cast<int64_t>(10))}},
                         shard_manager.get());
  auto edge = CreateEdge({{height, Value(static_cast<int64_t>(3))}, {age, Value(static_cast<int64_t>(15))}},
                         shard_manager.get());
  auto prop_keys_to_string = [](TypedValue t) {
    std::vector<std::string> keys;
    for (auto property : t.ValueList()) {
      keys.emplace_back(property.ValueString());
    }
    return keys;
  };
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", v1)), UnorderedElementsAre("height", "age"));
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", edge)), UnorderedElementsAre("height", "age"));
  ASSERT_THROW(EvaluateFunction("KEYS", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, Tail) {
  ASSERT_THROW(EvaluateFunction("TAIL"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("TAIL", TypedValue()).IsNull());
  auto argument = MakeTypedValueList();
  ASSERT_EQ(EvaluateFunction("TAIL", argument).ValueList().size(), 0U);
  argument = MakeTypedValueList(3, 4, true, "john");
  auto list = EvaluateFunction("TAIL", argument).ValueList();
  ASSERT_EQ(list.size(), 3U);
  ASSERT_EQ(list[0].ValueInt(), 4);
  ASSERT_EQ(list[1].ValueBool(), true);
  ASSERT_EQ(list[2].ValueString(), "john");
  ASSERT_THROW(EvaluateFunction("TAIL", 2), FunctionRuntimeException);
}

TEST_F(FunctionTest, UniformSample) {
  ASSERT_THROW(EvaluateFunction("UNIFORMSAMPLE"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("UNIFORMSAMPLE", TypedValue(), TypedValue()).IsNull());
  ASSERT_TRUE(EvaluateFunction("UNIFORMSAMPLE", TypedValue(), 1).IsNull());
  ASSERT_TRUE(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(), TypedValue()).IsNull());
  ASSERT_TRUE(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(), 1).IsNull());
  ASSERT_THROW(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), -1), FunctionRuntimeException);
  ASSERT_EQ(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 0).ValueList().size(), 0);
  ASSERT_EQ(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 2).ValueList().size(), 2);
  ASSERT_EQ(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 3).ValueList().size(), 3);
  ASSERT_EQ(EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 5).ValueList().size(), 5);
}

TEST_F(FunctionTest, Abs) {
  ASSERT_THROW(EvaluateFunction("ABS"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("ABS", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("ABS", -2).ValueInt(), 2);
  ASSERT_EQ(EvaluateFunction("ABS", -2.5).ValueDouble(), 2.5);
  ASSERT_THROW(EvaluateFunction("ABS", true), FunctionRuntimeException);
}

//// Test if log works. If it does then all functions wrapped with
//// WRAP_CMATH_FLOAT_FUNCTION macro should work and are not gonna be tested for
//// correctnes..
TEST_F(FunctionTest, Log) {
  ASSERT_THROW(EvaluateFunction("LOG"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("LOG", TypedValue()).IsNull());
  ASSERT_DOUBLE_EQ(EvaluateFunction("LOG", 2).ValueDouble(), log(2));
  ASSERT_DOUBLE_EQ(EvaluateFunction("LOG", 1.5).ValueDouble(), log(1.5));
  // Not portable, but should work on most platforms.
  ASSERT_TRUE(std::isnan(EvaluateFunction("LOG", -1.5).ValueDouble()));
  ASSERT_THROW(EvaluateFunction("LOG", true), FunctionRuntimeException);
}

//// Function Round wraps round from cmath and will work if FunctionTest.Log test
//// passes. This test is used to show behavior of round since it differs from
//// neo4j's round.
TEST_F(FunctionTest, Round) {
  ASSERT_THROW(EvaluateFunction("ROUND"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("ROUND", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("ROUND", -2).ValueDouble(), -2);
  ASSERT_EQ(EvaluateFunction("ROUND", -2.4).ValueDouble(), -2);
  ASSERT_EQ(EvaluateFunction("ROUND", -2.5).ValueDouble(), -3);
  ASSERT_EQ(EvaluateFunction("ROUND", -2.6).ValueDouble(), -3);
  ASSERT_EQ(EvaluateFunction("ROUND", 2.4).ValueDouble(), 2);
  ASSERT_EQ(EvaluateFunction("ROUND", 2.5).ValueDouble(), 3);
  ASSERT_EQ(EvaluateFunction("ROUND", 2.6).ValueDouble(), 3);
  ASSERT_THROW(EvaluateFunction("ROUND", true), FunctionRuntimeException);
}

// Check if wrapped functions are callable (check if everything was spelled
// correctly...). Wrapper correctnes is checked in FunctionTest.Log function
// test.
TEST_F(FunctionTest, WrappedMathFunctions) {
  for (auto function_name :
       {"FLOOR", "CEIL", "ROUND", "EXP", "LOG", "LOG10", "SQRT", "ACOS", "ASIN", "ATAN", "COS", "SIN", "TAN"}) {
    EvaluateFunction(function_name, 0.5);
  }
}

TEST_F(FunctionTest, Atan2) {
  ASSERT_THROW(EvaluateFunction("ATAN2"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("ATAN2", TypedValue(), 1).IsNull());
  ASSERT_TRUE(EvaluateFunction("ATAN2", 1, TypedValue()).IsNull());
  ASSERT_DOUBLE_EQ(EvaluateFunction("ATAN2", 2, -1.0).ValueDouble(), atan2(2, -1));
  ASSERT_THROW(EvaluateFunction("ATAN2", 3.0, true), FunctionRuntimeException);
}

TEST_F(FunctionTest, Sign) {
  ASSERT_THROW(EvaluateFunction("SIGN"), FunctionRuntimeException);
  ASSERT_TRUE(EvaluateFunction("SIGN", TypedValue()).IsNull());
  ASSERT_EQ(EvaluateFunction("SIGN", -2).ValueInt(), -1);
  ASSERT_EQ(EvaluateFunction("SIGN", -0.2).ValueInt(), -1);
  ASSERT_EQ(EvaluateFunction("SIGN", 0.0).ValueInt(), 0);
  ASSERT_EQ(EvaluateFunction("SIGN", 2.5).ValueInt(), 1);
  ASSERT_THROW(EvaluateFunction("SIGN", true), FunctionRuntimeException);
}

TEST_F(FunctionTest, E) {
  ASSERT_THROW(EvaluateFunction("E", 1), FunctionRuntimeException);
  ASSERT_DOUBLE_EQ(EvaluateFunction("E").ValueDouble(), M_E);
}

TEST_F(FunctionTest, Pi) {
  ASSERT_THROW(EvaluateFunction("PI", 1), FunctionRuntimeException);
  ASSERT_DOUBLE_EQ(EvaluateFunction("PI").ValueDouble(), M_PI);
}

TEST_F(FunctionTest, Rand) {
  ASSERT_THROW(EvaluateFunction("RAND", 1), FunctionRuntimeException);
  ASSERT_GE(EvaluateFunction("RAND").ValueDouble(), 0.0);
  ASSERT_LT(EvaluateFunction("RAND").ValueDouble(), 1.0);
}

using memgraph::functions::kStartsWith;
TEST_F(FunctionTest, StartsWith) {
  EXPECT_THROW(EvaluateFunction(kStartsWith), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kStartsWith, "a", TypedValue()).IsNull());
  EXPECT_THROW(EvaluateFunction(kStartsWith, TypedValue(), 1.3), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kStartsWith, "abc", "abc").ValueBool());
  EXPECT_TRUE(EvaluateFunction(kStartsWith, "abcdef", "abc").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kStartsWith, "abcdef", "aBc").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kStartsWith, "abc", "abcd").ValueBool());
}

using memgraph::functions::kEndsWith;
TEST_F(FunctionTest, EndsWith) {
  EXPECT_THROW(EvaluateFunction(kEndsWith), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kEndsWith, "a", TypedValue()).IsNull());
  EXPECT_THROW(EvaluateFunction(kEndsWith, TypedValue(), 1.3), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kEndsWith, "abc", "abc").ValueBool());
  EXPECT_TRUE(EvaluateFunction(kEndsWith, "abcdef", "def").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kEndsWith, "abcdef", "dEf").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kEndsWith, "bcd", "abcd").ValueBool());
}

using memgraph::functions::kContains;
TEST_F(FunctionTest, Contains) {
  EXPECT_THROW(EvaluateFunction(kContains), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kContains, "a", TypedValue()).IsNull());
  EXPECT_THROW(EvaluateFunction(kContains, TypedValue(), 1.3), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kContains, "abc", "abc").ValueBool());
  EXPECT_TRUE(EvaluateFunction(kContains, "abcde", "bcd").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kContains, "cde", "abcdef").ValueBool());
  EXPECT_FALSE(EvaluateFunction(kContains, "abcdef", "dEf").ValueBool());
}

TEST_F(FunctionTest, Assert) {
  // Invalid calls.
  ASSERT_THROW(EvaluateFunction("ASSERT"), FunctionRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", false, false), FunctionRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", "string", false), FunctionRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", false, "reason", true), FunctionRuntimeException);

  // Valid calls, assertion fails.
  ASSERT_THROW(EvaluateFunction("ASSERT", false), FunctionRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", false, "message"), FunctionRuntimeException);
  try {
    EvaluateFunction("ASSERT", false, "bbgba");
  } catch (FunctionRuntimeException &e) {
    ASSERT_TRUE(std::string(e.what()).find("bbgba") != std::string::npos);
  }

  // Valid calls, assertion passes.
  ASSERT_TRUE(EvaluateFunction("ASSERT", true).ValueBool());
  ASSERT_TRUE(EvaluateFunction("ASSERT", true, "message").ValueBool());
}

TEST_F(FunctionTest, Counter) {
  EXPECT_THROW(EvaluateFunction("COUNTER"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTER", "a"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTER", "a", "b"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTER", "a", "b", "c"), FunctionRuntimeException);

  EXPECT_EQ(EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 1);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c2", 0).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 2);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c2", 0).ValueInt(), 1);

  EXPECT_EQ(EvaluateFunction("COUNTER", "c3", -1).ValueInt(), -1);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c3", -1).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c3", -1).ValueInt(), 1);

  EXPECT_EQ(EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 5);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 10);

  EXPECT_EQ(EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), -5);
  EXPECT_EQ(EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), -10);

  EXPECT_THROW(EvaluateFunction("COUNTER", "c6", 0, 0), FunctionRuntimeException);
}

// TODO(kostasrim) Add this once we fix accessors CypherId() functions
// TEST_F(FunctionTest, Id) {
//  auto v = CreateVertex({}, shard_manager.get());
//  auto e = CreateEdge({}, shard_manager.get());
//  EXPECT_TRUE(EvaluateFunction("ID", TypedValue()).IsNull());
//  EXPECT_EQ(EvaluateFunction("ID", v).ValueInt(), 0);
//  EXPECT_EQ(EvaluateFunction("ID", e).ValueInt(), 0);
//  EXPECT_THROW(EvaluateFunction("ID"), FunctionRuntimeException);
//  EXPECT_THROW(EvaluateFunction("ID", 0), FunctionRuntimeException);
//  EXPECT_THROW(EvaluateFunction("ID", v, e), FunctionRuntimeException);
//}

TEST_F(FunctionTest, ToStringNull) { EXPECT_TRUE(EvaluateFunction("TOSTRING", TypedValue()).IsNull()); }

TEST_F(FunctionTest, ToStringString) {
  EXPECT_EQ(EvaluateFunction("TOSTRING", "").ValueString(), "");
  EXPECT_EQ(EvaluateFunction("TOSTRING", "this is a string").ValueString(), "this is a string");
}

TEST_F(FunctionTest, ToStringInteger) {
  EXPECT_EQ(EvaluateFunction("TOSTRING", -23321312).ValueString(), "-23321312");
  EXPECT_EQ(EvaluateFunction("TOSTRING", 0).ValueString(), "0");
  EXPECT_EQ(EvaluateFunction("TOSTRING", 42).ValueString(), "42");
}

TEST_F(FunctionTest, ToStringDouble) {
  EXPECT_EQ(EvaluateFunction("TOSTRING", -42.42).ValueString(), "-42.420000");
  EXPECT_EQ(EvaluateFunction("TOSTRING", 0.0).ValueString(), "0.000000");
  EXPECT_EQ(EvaluateFunction("TOSTRING", 238910.2313217).ValueString(), "238910.231322");
}

TEST_F(FunctionTest, ToStringBool) {
  EXPECT_EQ(EvaluateFunction("TOSTRING", true).ValueString(), "true");
  EXPECT_EQ(EvaluateFunction("TOSTRING", false).ValueString(), "false");
}

// TEST_F(FunctionTest, ToStringDate) {
//   const auto date = memgraph::utils::Date({1970, 1, 2});
//   EXPECT_EQ(EvaluateFunction("TOSTRING", date).ValueString(), "1970-01-02");
// }
//
// TEST_F(FunctionTest, ToStringLocalTime) {
//   const auto lt = memgraph::utils::LocalTime({13, 2, 40, 100, 50});
//   EXPECT_EQ(EvaluateFunction("TOSTRING", lt).ValueString(), "13:02:40.100050");
// }
//
// TEST_F(FunctionTest, ToStringLocalDateTime) {
//   const auto ldt = memgraph::utils::LocalDateTime({1970, 1, 2}, {23, 02, 59});
//   EXPECT_EQ(EvaluateFunction("TOSTRING", ldt).ValueString(), "1970-01-02T23:02:59.000000");
// }
//
// TEST_F(FunctionTest, ToStringDuration) {
//   memgraph::utils::Duration duration{{.minute = 2, .second = 2, .microsecond = 33}};
//   EXPECT_EQ(EvaluateFunction("TOSTRING", duration).ValueString(), "P0DT0H2M2.000033S");
// }
//
TEST_F(FunctionTest, ToStringExceptions) {
  EXPECT_THROW(EvaluateFunction("TOSTRING", 1, 2, 3), FunctionRuntimeException);
}
//
// TEST_F(FunctionTest, TimestampVoid) {
//   ctx.timestamp = 42;
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP").ValueInt(), 42);
// }
//
// TEST_F(FunctionTest, TimestampDate) {
//   ctx.timestamp = 42;
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP", memgraph::utils::Date({1970, 1, 1})).ValueInt(), 0);
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP", memgraph::utils::Date({1971, 1, 1})).ValueInt(), 31536000000000);
// }
//
// TEST_F(FunctionTest, TimestampLocalTime) {
//   ctx.timestamp = 42;
//   const memgraph::utils::LocalTime time(10000);
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP", time).ValueInt(), 10000);
// }
//
// TEST_F(FunctionTest, TimestampLocalDateTime) {
//   ctx.timestamp = 42;
//   const memgraph::utils::LocalDateTime time(20000);
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP", time).ValueInt(), 20000);
// }
//
// TEST_F(FunctionTest, TimestampDuration) {
//   ctx.timestamp = 42;
//   const memgraph::utils::Duration time(20000);
//   EXPECT_EQ(EvaluateFunction("TIMESTAMP", time).ValueInt(), 20000);
// }
//
// TEST_F(FunctionTest, TimestampExceptions) {
//   ctx.timestamp = 42;
//   EXPECT_THROW(EvaluateFunction("TIMESTAMP", 1).ValueInt(), ExpressionRuntimeException);
// }
//
TEST_F(FunctionTest, Left) {
  EXPECT_THROW(EvaluateFunction("LEFT"), FunctionRuntimeException);

  EXPECT_TRUE(EvaluateFunction("LEFT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("LEFT", TypedValue(), 10).IsNull());
  EXPECT_THROW(EvaluateFunction("LEFT", TypedValue(), -10), FunctionRuntimeException);

  EXPECT_EQ(EvaluateFunction("LEFT", "memgraph", 0).ValueString(), "");
  EXPECT_EQ(EvaluateFunction("LEFT", "memgraph", 3).ValueString(), "mem");
  EXPECT_EQ(EvaluateFunction("LEFT", "memgraph", 1000).ValueString(), "memgraph");
  EXPECT_THROW(EvaluateFunction("LEFT", "memgraph", -10), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("LEFT", "memgraph", "graph"), FunctionRuntimeException);

  EXPECT_THROW(EvaluateFunction("LEFT", 132, 10), FunctionRuntimeException);
}

TEST_F(FunctionTest, Right) {
  EXPECT_THROW(EvaluateFunction("RIGHT"), FunctionRuntimeException);

  EXPECT_TRUE(EvaluateFunction("RIGHT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("RIGHT", TypedValue(), 10).IsNull());
  EXPECT_THROW(EvaluateFunction("RIGHT", TypedValue(), -10), FunctionRuntimeException);

  EXPECT_EQ(EvaluateFunction("RIGHT", "memgraph", 0).ValueString(), "");
  EXPECT_EQ(EvaluateFunction("RIGHT", "memgraph", 3).ValueString(), "aph");
  EXPECT_EQ(EvaluateFunction("RIGHT", "memgraph", 1000).ValueString(), "memgraph");
  EXPECT_THROW(EvaluateFunction("RIGHT", "memgraph", -10), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("RIGHT", "memgraph", "graph"), FunctionRuntimeException);

  EXPECT_THROW(EvaluateFunction("RIGHT", 132, 10), FunctionRuntimeException);
}

TEST_F(FunctionTest, Trimming) {
  EXPECT_TRUE(EvaluateFunction("LTRIM", TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("RTRIM", TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("TRIM", TypedValue()).IsNull());

  EXPECT_EQ(EvaluateFunction("LTRIM", "  abc    ").ValueString(), "abc    ");
  EXPECT_EQ(EvaluateFunction("RTRIM", " abc ").ValueString(), " abc");
  EXPECT_EQ(EvaluateFunction("TRIM", "abc").ValueString(), "abc");

  EXPECT_THROW(EvaluateFunction("LTRIM", "x", "y"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("RTRIM", "x", "y"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TRIM", "x", "y"), FunctionRuntimeException);
}

TEST_F(FunctionTest, Reverse) {
  EXPECT_TRUE(EvaluateFunction("REVERSE", TypedValue()).IsNull());
  EXPECT_EQ(EvaluateFunction("REVERSE", "abc").ValueString(), "cba");
  EXPECT_THROW(EvaluateFunction("REVERSE", "x", "y"), FunctionRuntimeException);
}

TEST_F(FunctionTest, Replace) {
  EXPECT_THROW(EvaluateFunction("REPLACE"), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction("REPLACE", TypedValue(), "l", "w").IsNull());
  EXPECT_TRUE(EvaluateFunction("REPLACE", "hello", TypedValue(), "w").IsNull());
  EXPECT_TRUE(EvaluateFunction("REPLACE", "hello", "l", TypedValue()).IsNull());
  EXPECT_EQ(EvaluateFunction("REPLACE", "hello", "l", "w").ValueString(), "hewwo");

  EXPECT_THROW(EvaluateFunction("REPLACE", 1, "l", "w"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("REPLACE", "hello", 1, "w"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("REPLACE", "hello", "l", 1), FunctionRuntimeException);
}

TEST_F(FunctionTest, Split) {
  EXPECT_THROW(EvaluateFunction("SPLIT"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("SPLIT", "one,two", 1), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("SPLIT", 1, "one,two"), FunctionRuntimeException);

  EXPECT_TRUE(EvaluateFunction("SPLIT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("SPLIT", "one,two", TypedValue()).IsNull());
  EXPECT_TRUE(EvaluateFunction("SPLIT", TypedValue(), ",").IsNull());

  auto result = EvaluateFunction("SPLIT", "one,two", ",");
  EXPECT_TRUE(result.IsList());
  EXPECT_EQ(result.ValueList()[0].ValueString(), "one");
  EXPECT_EQ(result.ValueList()[1].ValueString(), "two");
}

TEST_F(FunctionTest, Substring) {
  EXPECT_THROW(EvaluateFunction("SUBSTRING"), FunctionRuntimeException);

  EXPECT_TRUE(EvaluateFunction("SUBSTRING", TypedValue(), 0, 10).IsNull());
  EXPECT_THROW(EvaluateFunction("SUBSTRING", TypedValue(), TypedValue()), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("SUBSTRING", TypedValue(), -10), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("SUBSTRING", TypedValue(), 0, TypedValue()), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("SUBSTRING", TypedValue(), 0, -10), FunctionRuntimeException);

  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 2).ValueString(), "llo");
  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 10).ValueString(), "");
  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 2, 0).ValueString(), "");
  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 1, 3).ValueString(), "ell");
  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 1, 4).ValueString(), "ello");
  EXPECT_EQ(EvaluateFunction("SUBSTRING", "hello", 1, 10).ValueString(), "ello");
}

TEST_F(FunctionTest, ToLower) {
  EXPECT_THROW(EvaluateFunction("TOLOWER"), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction("TOLOWER", TypedValue()).IsNull());
  EXPECT_EQ(EvaluateFunction("TOLOWER", "Ab__C").ValueString(), "ab__c");
}

TEST_F(FunctionTest, ToUpper) {
  EXPECT_THROW(EvaluateFunction("TOUPPER"), FunctionRuntimeException);
  EXPECT_TRUE(EvaluateFunction("TOUPPER", TypedValue()).IsNull());
  EXPECT_EQ(EvaluateFunction("TOUPPER", "Ab__C").ValueString(), "AB__C");
}

TEST_F(FunctionTest, ToByteString) {
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", 42), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", TypedValue()), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", "", 42), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", "ff"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", "00"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("TOBYTESTRING", "0xG"), FunctionRuntimeException);
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "").ValueString(), "");
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "0x").ValueString(), "");
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "0X").ValueString(), "");
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "0x0123456789aAbBcCdDeEfF").ValueString(),
            "\x01\x23\x45\x67\x89\xAA\xBB\xCC\xDD\xEE\xFF");
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "0x042").ValueString().size(), 2);
  EXPECT_EQ(EvaluateFunction("TOBYTESTRING", "0x042").ValueString(),
            memgraph::utils::pmr::string("\x00\x42", 2, memgraph::utils::NewDeleteResource()));
}

TEST_F(FunctionTest, FromByteString) {
  EXPECT_THROW(EvaluateFunction("FROMBYTESTRING"), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("FROMBYTESTRING", 42), FunctionRuntimeException);
  EXPECT_THROW(EvaluateFunction("FROMBYTESTRING", TypedValue()), FunctionRuntimeException);
  EXPECT_EQ(EvaluateFunction("FROMBYTESTRING", "").ValueString(), "");
  auto bytestring = EvaluateFunction("TOBYTESTRING", "0x123456789aAbBcCdDeEfF");
  EXPECT_EQ(EvaluateFunction("FROMBYTESTRING", bytestring).ValueString(), "0x0123456789aabbccddeeff");
  EXPECT_EQ(EvaluateFunction("FROMBYTESTRING", std::string("\x00\x42", 2)).ValueString(), "0x0042");
}

// TEST_F(FunctionTest, Date) {
//   const auto unix_epoch = memgraph::utils::Date({1970, 1, 1});
//   EXPECT_EQ(EvaluateFunction("DATE", "1970-01-01").ValueDate(), unix_epoch);
//   const auto map_param = TypedValue(
//       std::map<std::string, TypedValue>{{"year", TypedValue(1970)}, {"month", TypedValue(1)}, {"day",
//       TypedValue(1)}});
//   EXPECT_EQ(EvaluateFunction("DATE", map_param).ValueDate(), unix_epoch);
//   const auto today = memgraph::utils::CurrentDate();
//   EXPECT_EQ(EvaluateFunction("DATE").ValueDate(), today);
//
//   EXPECT_THROW(EvaluateFunction("DATE", "{}"), memgraph::utils::BasicException);
//   EXPECT_THROW(EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"years", TypedValue(1970)}}),
//                ExpressionRuntimeException);
//   EXPECT_THROW(EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"mnths", TypedValue(1970)}}),
//                ExpressionRuntimeException);
//   EXPECT_THROW(EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"dayz", TypedValue(1970)}}),
//                ExpressionRuntimeException);
// }
//
// TEST_F(FunctionTest, LocalTime) {
//   const auto local_time = memgraph::utils::LocalTime({13, 3, 2, 0, 0});
//   EXPECT_EQ(EvaluateFunction("LOCALTIME", "130302").ValueLocalTime(), local_time);
//   const auto one_sec_in_microseconds = 1000000;
//   const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"hour", TypedValue(1)},
//                                                                       {"minute", TypedValue(2)},
//                                                                       {"second", TypedValue(3)},
//                                                                       {"millisecond", TypedValue(4)},
//                                                                       {"microsecond", TypedValue(5)}});
//   EXPECT_EQ(EvaluateFunction("LOCALTIME", map_param).ValueLocalTime(), memgraph::utils::LocalTime({1, 2, 3, 4, 5}));
//   const auto today = memgraph::utils::CurrentLocalTime();
//   EXPECT_NEAR(EvaluateFunction("LOCALTIME").ValueLocalTime().MicrosecondsSinceEpoch(),
//   today.MicrosecondsSinceEpoch(),
//               one_sec_in_microseconds);
//
//   EXPECT_THROW(EvaluateFunction("LOCALTIME", "{}"), memgraph::utils::BasicException);
//   EXPECT_THROW(EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"hous",
//   TypedValue(1970)}})),
//                ExpressionRuntimeException);
//   EXPECT_THROW(
//       EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"minut", TypedValue(1970)}})),
//       ExpressionRuntimeException);
//   EXPECT_THROW(
//       EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
//       ExpressionRuntimeException);
// }
//
// TEST_F(FunctionTest, LocalDateTime) {
//   const auto local_date_time = memgraph::utils::LocalDateTime({1970, 1, 1}, {13, 3, 2, 0, 0});
//   EXPECT_EQ(EvaluateFunction("LOCALDATETIME", "1970-01-01T13:03:02").ValueLocalDateTime(), local_date_time);
//   const auto today = memgraph::utils::CurrentLocalDateTime();
//   const auto one_sec_in_microseconds = 1000000;
//   const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"year", TypedValue(1972)},
//                                                                       {"month", TypedValue(2)},
//                                                                       {"day", TypedValue(3)},
//                                                                       {"hour", TypedValue(4)},
//                                                                       {"minute", TypedValue(5)},
//                                                                       {"second", TypedValue(6)},
//                                                                       {"millisecond", TypedValue(7)},
//                                                                       {"microsecond", TypedValue(8)}});
//
//   EXPECT_EQ(EvaluateFunction("LOCALDATETIME", map_param).ValueLocalDateTime(),
//             memgraph::utils::LocalDateTime({1972, 2, 3}, {4, 5, 6, 7, 8}));
//   EXPECT_NEAR(EvaluateFunction("LOCALDATETIME").ValueLocalDateTime().MicrosecondsSinceEpoch(),
//               today.MicrosecondsSinceEpoch(), one_sec_in_microseconds);
//   EXPECT_THROW(EvaluateFunction("LOCALDATETIME", "{}"), memgraph::utils::BasicException);
//   EXPECT_THROW(
//       EvaluateFunction("LOCALDATETIME", TypedValue(std::map<std::string, TypedValue>{{"hours", TypedValue(1970)}})),
//       ExpressionRuntimeException);
//   EXPECT_THROW(
//       EvaluateFunction("LOCALDATETIME", TypedValue(std::map<std::string, TypedValue>{{"seconds",
//       TypedValue(1970)}})), ExpressionRuntimeException);
// }
//
// TEST_F(FunctionTest, Duration) {
//   const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"day", TypedValue(3)},
//                                                                       {"hour", TypedValue(4)},
//                                                                       {"minute", TypedValue(5)},
//                                                                       {"second", TypedValue(6)},
//                                                                       {"millisecond", TypedValue(7)},
//                                                                       {"microsecond", TypedValue(8)}});
//
//   EXPECT_EQ(EvaluateFunction("DURATION", map_param).ValueDuration(), memgraph::utils::Duration({3, 4, 5, 6, 7, 8}));
//   EXPECT_THROW(EvaluateFunction("DURATION", "{}"), memgraph::utils::BasicException);
//   EXPECT_THROW(EvaluateFunction("DURATION", TypedValue(std::map<std::string, TypedValue>{{"hours",
//   TypedValue(1970)}})),
//                ExpressionRuntimeException);
//   EXPECT_THROW(
//       EvaluateFunction("DURATION", TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
//       ExpressionRuntimeException);
//
//   const auto map_param_negative = TypedValue(std::map<std::string, TypedValue>{{"day", TypedValue(-3)},
//                                                                                {"hour", TypedValue(-4)},
//                                                                                {"minute", TypedValue(-5)},
//                                                                                {"second", TypedValue(-6)},
//                                                                                {"millisecond", TypedValue(-7)},
//                                                                                {"microsecond", TypedValue(-8)}});
//   EXPECT_EQ(EvaluateFunction("DURATION", map_param_negative).ValueDuration(),
//             memgraph::utils::Duration({-3, -4, -5, -6, -7, -8}));
//
//   EXPECT_EQ(EvaluateFunction("DURATION", "P4DT4H5M6.2S").ValueDuration(),
//             memgraph::utils::Duration({4, 4, 5, 6, 0, 200000}));
//   EXPECT_EQ(EvaluateFunction("DURATION", "P3DT4H5M6.100S").ValueDuration(),
//             memgraph::utils::Duration({3, 4, 5, 6, 0, 100000}));
//   EXPECT_EQ(EvaluateFunction("DURATION", "P3DT4H5M6.100110S").ValueDuration(),
//             memgraph::utils::Duration({3, 4, 5, 6, 100, 110}));
// }
}  // namespace
