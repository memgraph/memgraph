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
// #include "storage/v3/bindings/ast/ast.hpp"
// #include "storage/v3/bindings/bindings.hpp"
// #include "storage/v3/bindings/cypher_main_visitor.hpp"
// #include "storage/v3/bindings/db_accessor.hpp"
// #include "storage/v3/bindings/eval.hpp"
// #include "storage/v3/bindings/frame.hpp"
// #include "storage/v3/bindings/symbol_generator.hpp"
// #include "storage/v3/bindings/symbol_table.hpp"
// #include "storage/v3/bindings/typed_value.hpp"
#include "query/v2/bindings/cypher_main_visitor.hpp"
#include "query/v2/bindings/eval.hpp"
#include "query/v2/bindings/frame.hpp"
#include "query/v2/bindings/symbol_generator.hpp"
#include "query/v2/bindings/symbol_table.hpp"
#include "query/v2/bindings/typed_value.hpp"
#include "query/v2/db_accessor.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "storage/v3/id_types.hpp"
// #include "storage/v3/key_store.hpp"
// #include "storage/v3/property_value.hpp"
// #include "storage/v3/schemas.hpp"
// #include "storage/v3/shard.hpp"
#include "expr/ast/pretty_print_ast_to_original_expression.hpp"
#include "utils/string.hpp"

// #NoCommit keep one
// #include "query/frontend/ast/pretty_print.hpp"
// #include "query/v2/bindings/pretty_print.hpp"
// #include "query/v2/plan/pretty_print.hpp"

// Example from jure: https://github.com/memgraph/memgraph/blob/use-expr/tests/unit/storage_v3_expr_usage.cpp
// const std::string expr1{"node.prop2 > 0 AND node.prop2 < 10"};

// // Parse stuff
// memgraph::frontend::opencypher::Parser<memgraph::frontend::opencypher::ParserOpTag::EXPRESSION> parser(expr1);
// expr::ParsingContext pc;
// CypherMainVisitor visitor(pc, &storage);

// auto *ast = parser.tree();
// auto expr = visitor.visit(ast);

namespace memgraph::query::v2::test {

class ExpressiontoStringTest : public ::testing::TestWithParam<std::pair<std::string, std::string>> {
 protected:
  AstStorage storage;
  memgraph::utils::MonotonicBufferResource mem{1024};
  EvaluationContext ctx{.memory = &mem};
  SymbolTable symbol_table_;

  storage::v3::LabelId primary_label{storage::v3::LabelId::FromInt(1)};
  storage::v3::PropertyId primary_property{storage::v3::PropertyId::FromInt(2)};
  storage::v3::PrimaryKey min_pk{storage::v3::PropertyValue(0)};
  std::vector<storage::v3::SchemaProperty> schema_property_vector = {
      storage::v3::SchemaProperty{primary_property, common::SchemaType::INT}};

  storage::v3::Shard db{primary_label, min_pk, std::nullopt, schema_property_vector};
  storage::v3::Shard::Accessor storage_dba{db.Access(GetNextHlc())};
  DbAccessor dba{&storage_dba};

  Frame frame_{128};
  ExpressionEvaluator eval{&frame_, symbol_table_, ctx, &dba, storage::v3::View::NEW};

  coordinator::Hlc last_hlc{0, io::Time{}};

  //   std::vector<storage::v3::PropertyId> NamesToProperties(const std::vector<std::string> &property_names) {
  //     std::vector<storage::v3::PropertyId> properties;
  //     properties.reserve(property_names.size());
  //     for (const auto &name : property_names) {
  //       properties.push_back(dba.NameToProperty(name));
  //     }
  //     return properties;
  //   }

  //   std::vector<LabelId> NamesToLabels(const std::vector<std::string> &label_names) {
  //     std::vector<LabelId> labels;
  //     labels.reserve(label_names.size());
  //     for (const auto &name : label_names) {
  //       labels.push_back(dba.NameToLabel(name));
  //     }
  //     return labels;
  //   }

  Identifier *CreateIdentifierWithValue(std::string name, const TypedValue &value) {
    auto *id = storage.Create<Identifier>(name, true);
    auto symbol = symbol_table_.CreateSymbol(name, true);
    id->MapTo(symbol);
    frame_[symbol] = value;
    return id;
  }

  template <class TExpression>
  auto Eval(TExpression *expr) {
    ctx.properties = std::vector<storage::v3::PropertyId>{2};  //;NamesToProperties(storage.properties_);
    ctx.labels = std::vector<storage::v3::LabelId>{1};         //    NamesToLabels(storage.labels_);
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

TEST_P(ExpressiontoStringTest, Example) {
  const auto [original_expression, expected_expression] = GetParam();

  memgraph::frontend::opencypher::Parser<frontend::opencypher::ParserOpTag::EXPRESSION> parser(original_expression);
  expr::ParsingContext pc;
  CypherMainVisitor visitor(pc, &storage);

  auto *ast = parser.tree();
  auto expression = visitor.visit(ast);

  const auto rewritten_expression =
      expr::ExpressiontoStringWhileReplacingNodeAndEdgeSymbols(std::any_cast<Expression *>(expression));

  ASSERT_EQ(rewritten_expression, expected_expression);
  //
}

INSTANTIATE_TEST_CASE_P(PARAMETER, ExpressiontoStringTest,
                        ::testing::Values(std::make_pair(std::string("2 + 1 + 5 + 2"), std::string("2 + 1 + 5 + 2"))));

// NOLINTNEXTLINE(hicpp-special-member-functions)
// TEST_F(ExpressionEvaluatorUsageTest, DummyExample2) {
//   // dummy
//   const auto original_expression{"(n.prop > 3 and n.prop2 < 3) or n.prop3='perfect'"};
//   const auto expected_expression = {""};

//   // const std::string expr1{"(+ (- (+ 2 1) 3) 2)"};
//   //  Parse stuff
//   memgraph::frontend::opencypher::Parser<frontend::opencypher::ParserOpTag::EXPRESSION> parser(original_expression);
//   expr::ParsingContext pc;
//   CypherMainVisitor visitor(pc, &storage);

//   auto *ast = parser.tree();
//   auto expression = visitor.visit(ast);

//   auto rewriten_expression =
//       expr::ExpressiontoStringWhileReplacingNodeAndEdgeSymbols(std::any_cast<Expression *>(expression));

//   ASSERT_EQ(rewriten_expression, expected_expression);
//   //
// }

}  // namespace memgraph::query::v2::test
