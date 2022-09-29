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
#include <limits>
#include <thread>

#include <gtest/gtest.h>

#include <coordinator/coordinator.hpp>
#include <coordinator/coordinator_client.hpp>
#include <coordinator/hybrid_logical_clock.hpp>
#include <coordinator/shard_map.hpp>
#include <io/local_transport/local_system.hpp>
#include <io/local_transport/local_transport.hpp>
#include <io/rsm/rsm_client.hpp>
#include <io/transport.hpp>
#include <machine_manager/machine_config.hpp>
#include <machine_manager/machine_manager.hpp>
#include "common/types.hpp"
#include "exceptions.hpp"
#include "io/rsm/rsm_client.hpp"
#include "parser/opencypher/parser.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/bindings.hpp"
#include "storage/v3/bindings/cypher_main_visitor.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/eval.hpp"
#include "storage/v3/bindings/frame.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/bindings/typed_value.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/shard.hpp"
#include "utils/string.hpp"

namespace memgraph::storage::v3::test {

class ExpressionEvaluatorUsageTest : public ::testing::Test {
 protected:
  LabelId primary_label{LabelId::FromInt(1)};
  PropertyId primary_property{PropertyId::FromInt(2)};
  PrimaryKey min_pk{PropertyValue(0)};

  Shard db{primary_label, min_pk, std::nullopt};
  Shard::Accessor storage_dba{db.Access(GetNextHlc())};
  DbAccessor dba{&storage_dba};

  AstStorage storage;
  memgraph::utils::MonotonicBufferResource mem{1024};
  EvaluationContext ctx{.memory = &mem};
  SymbolTable symbol_table_;

  Frame frame_{128};
  ExpressionEvaluator eval{&frame_, symbol_table_, ctx, &dba, View::NEW};

  coordinator::Hlc last_hlc{0, io::Time{}};

  void SetUp() override {
    db.StoreMapping({{1, "label"}, {2, "property"}});
    ASSERT_TRUE(
        db.CreateSchema(primary_label, {storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}}));
  }

  std::vector<PropertyId> NamesToProperties(const std::vector<std::string> &property_names) {
    std::vector<PropertyId> properties;
    properties.reserve(property_names.size());
    for (const auto &name : property_names) {
      properties.push_back(dba.NameToProperty(name));
    }
    return properties;
  }

  std::vector<LabelId> NamesToLabels(const std::vector<std::string> &label_names) {
    std::vector<LabelId> labels;
    labels.reserve(label_names.size());
    for (const auto &name : label_names) {
      labels.push_back(dba.NameToLabel(name));
    }
    return labels;
  }

  Identifier *CreateIdentifierWithValue(std::string name, const TypedValue &value) {
    auto *id = storage.Create<Identifier>(name, true);
    auto symbol = symbol_table_.CreateSymbol(name, true);
    id->MapTo(symbol);
    frame_[symbol] = value;
    return id;
  }

  template <class TExpression>
  auto Eval(TExpression *expr) {
    ctx.properties = NamesToProperties(storage.properties_);
    ctx.labels = NamesToLabels(storage.labels_);
    auto value = expr->Accept(eval);
    EXPECT_EQ(value.GetMemoryResource(), &mem) << "ExpressionEvaluator must use the MemoryResource from "
                                                  "EvaluationContext for allocations!";
    return value;
  }

  coordinator::Hlc GetNextHlc() {
    ++last_hlc.logical_id;
    last_hlc.coordinator_wall_clock += std::chrono::seconds(1);
    return last_hlc;
  }
};

class StrippedQuery {
 public:
  /**
   * Strips the input query and stores stripped query, stripped arguments and
   * stripped query hash.
   *
   * @param query Input query.
   */
  explicit StrippedQuery(const std::string &query);

  /**
   * Copy constructor is deleted because we don't want to make unnecessary
   * copies of this object (copying of string and vector could be expensive)
   */
  StrippedQuery(const StrippedQuery &other) = delete;
  StrippedQuery &operator=(const StrippedQuery &other) = delete;

  /**
   * Move is allowed operation because it is not expensive and we can
   * move the object after it was created.
   */
  StrippedQuery(StrippedQuery &&other) = default;
  StrippedQuery &operator=(StrippedQuery &&other) = default;

  const std::string &query() const { return query_; }
  const auto &original_query() const { return original_; }
  const auto &literals() const { return literals_; }
  const auto &named_expressions() const { return named_exprs_; }
  const auto &parameters() const { return parameters_; }
  uint64_t hash() const { return hash_; }

 private:
  // Return len of matched keyword if something is matched, otherwise 0.
  int MatchKeyword(int start) const;
  int MatchString(int start) const;
  int MatchSpecial(int start) const;
  int MatchDecimalInt(int start) const;
  int MatchOctalInt(int start) const;
  int MatchHexadecimalInt(int start) const;
  int MatchReal(int start) const;
  int MatchParameter(int start) const;
  int MatchEscapedName(int start) const;
  int MatchUnescapedName(int start) const;
  int MatchWhitespaceAndComments(int start) const;

  // Original query.
  std::string original_;

  // Stripped query.
  std::string query_;

  // Token positions of stripped out literals mapped to their values.
  // TODO: Parameters class really doesn't provide anything interesting. This
  // could be changed to std::unordered_map, but first we need to rewrite (or
  // get rid of) hardcoded queries which expect Parameters.
  Parameters literals_;

  // Token positions of query parameters mapped to their names.
  std::unordered_map<int, std::string> parameters_;

  // Token positions of nonaliased named expressions in return statement mapped
  // to their original (unstripped) string.
  std::unordered_map<int, std::string> named_exprs_;

  // Hash based on the stripped query.
  uint64_t hash_;
};

TEST_F(ExpressionEvaluatorUsageTest, DummyExample) {
  // dummy
  const std::string expr1{"2+1-3+2"};

  // Parse stuff
  memgraph::frontend::opencypher::Parser<frontend::opencypher::ParserOpTag::EXPRESSION> parser(expr1);
  expr::ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  auto ladida = visitor.visit(ast);
  auto res1 = Eval(std::any_cast<Expression *>(ladida));
  EXPECT_EQ(res1.ValueInt(), 2);
}

TEST_F(ExpressionEvaluatorUsageTest, PropertyLookup) {
  db.StoreMapping({{1, "label"}, {2, "property"}, {3, "prop2"}});
  const auto prop2 = PropertyId::FromUint(3);

  auto v1 = *dba.InsertVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(1)}});
  ASSERT_TRUE(v1.SetPropertyAndValidate(prop2, PropertyValue(5)).HasValue());

  auto v2 = *dba.InsertVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(2)}});
  ASSERT_TRUE(v2.SetPropertyAndValidate(prop2, PropertyValue(5)).HasValue());

  auto v3 = *dba.InsertVertexAndValidate(primary_label, {}, {{primary_property, PropertyValue(3)}});
  ASSERT_TRUE(v3.SetPropertyAndValidate(prop2, PropertyValue(5)).HasValue());

  // Property filtering
  // const std::string expr1{"n.prop2 > 0"};
  const std::string expr1{"node.prop2 > 0 AND node.prop2 < 10"};

  // Parse stuff
  memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(expr1);
  expr::ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  auto expr = visitor.visit(ast);

  static constexpr const char *node_name = "node";
  static constexpr const char *edge_name = "edge";

  static Identifier node_identifier = Identifier(std::string(node_name), false);
  bool is_node_identifier_present = false;
  static Identifier edge_identifier = Identifier(std::string(edge_name), false);
  bool is_edge_identifier_present = false;

  std::vector<Identifier *> identifiers;

  if (expr1.find(node_name) != std::string::npos) {
    is_node_identifier_present = true;
    identifiers.push_back(&node_identifier);
  }
  if (expr1.find(edge_name) != std::string::npos) {
    is_edge_identifier_present = true;
    identifiers.push_back(&edge_identifier);
  }

  expr::SymbolGenerator symbol_generator(&symbol_table_, identifiers);
  (std::any_cast<Expression *>(expr))->Accept(symbol_generator);

  if (is_node_identifier_present) {
    frame_[symbol_table_.at(node_identifier)] = v2;  // vertex accessor
  }
  if (is_edge_identifier_present) {
    frame_[symbol_table_.at(edge_identifier)] = v1;  // edge accessor
  }

  auto res1 = Eval(std::any_cast<Expression *>(expr));
  ASSERT_TRUE(res1.ValueBool());
}

}  // namespace memgraph::storage::v3::test
