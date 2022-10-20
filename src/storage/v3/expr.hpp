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

#include <vector>

#include "db_accessor.hpp"
#include "opencypher/parser.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/bindings/ast/ast.hpp"
#include "storage/v3/bindings/bindings.hpp"
#include "storage/v3/bindings/cypher_main_visitor.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/bindings/eval.hpp"
#include "storage/v3/bindings/frame.hpp"
#include "storage/v3/bindings/symbol_generator.hpp"
#include "storage/v3/bindings/symbol_table.hpp"
#include "storage/v3/bindings/typed_value.hpp"

namespace memgraph::storage::v3 {

memgraph::msgs::Value ConstructValueVertex(const memgraph::storage::v3::VertexAccessor &acc, View view);

msgs::Value ConstructValueEdge(const EdgeAccessor &acc, View view);

msgs::Value FromTypedValueToValue(TypedValue &&tv);

std::vector<msgs::Value> ConvertToValueVectorFromTypedValueVector(std::vector<TypedValue> &&vec);

std::vector<PropertyId> NamesToProperties(const std::vector<std::string> &property_names, DbAccessor &dba);

std::vector<LabelId> NamesToLabels(const std::vector<std::string> &label_names, DbAccessor &dba);

template <class TExpression>
auto Eval(TExpression *expr, EvaluationContext &ctx, AstStorage &storage, ExpressionEvaluator &eval, DbAccessor &dba) {
  ctx.properties = NamesToProperties(storage.properties_, dba);
  ctx.labels = NamesToLabels(storage.labels_, dba);
  auto value = expr->Accept(eval);
  return value;
}

std::any ParseExpression(const std::string &expr, AstStorage &storage);

TypedValue ComputeExpression(DbAccessor &dba, const std::optional<VertexAccessor> &v_acc,
                             const std::optional<EdgeAccessor> &e_acc, const std::string &expression,
                             std::string_view node_name, std::string_view edge_name);

}  // namespace memgraph::storage::v3
