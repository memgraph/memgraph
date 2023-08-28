// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/pretty_print.hpp"
#include <variant>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/pretty_print.hpp"
#include "query/plan/operator.hpp"
#include "utils/string.hpp"

namespace memgraph::query::plan {

namespace impl {

///////////////////////////////////////////////////////////////////////////////
//
// PlanToJsonVisitor implementation
//
// The JSON formatted plan is consumed (or will be) by Memgraph Lab, and
// therefore should not be changed before synchronizing with whoever is
// maintaining Memgraph Lab. Hopefully, one day integration tests will exist and
// there will be no need to be super careful.

using nlohmann::json;

//////////////////////////// HELPER FUNCTIONS /////////////////////////////////
// TODO: It would be nice to have enum->string functions auto-generated.
std::string ToString(EdgeAtom::Direction dir) {
  switch (dir) {
    case EdgeAtom::Direction::BOTH:
      return "both";
    case EdgeAtom::Direction::IN:
      return "in";
    case EdgeAtom::Direction::OUT:
      return "out";
  }
}

std::string ToString(EdgeAtom::Type type) {
  switch (type) {
    case EdgeAtom::Type::BREADTH_FIRST:
      return "bfs";
    case EdgeAtom::Type::DEPTH_FIRST:
      return "dfs";
    case EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      return "wsp";
    case EdgeAtom::Type::ALL_SHORTEST_PATHS:
      return "asp";
    case EdgeAtom::Type::SINGLE:
      return "single";
  }
}

std::string ToString(Ordering ord) {
  switch (ord) {
    case Ordering::ASC:
      return "asc";
    case Ordering::DESC:
      return "desc";
  }
}

json ToJson(Expression *expression) {
  std::stringstream sstr;
  PrintExpression(expression, &sstr);
  return sstr.str();
}

json ToJson(const utils::Bound<Expression *> &bound) {
  json json;
  switch (bound.type()) {
    case utils::BoundType::INCLUSIVE:
      json["type"] = "inclusive";
      break;
    case utils::BoundType::EXCLUSIVE:
      json["type"] = "exclusive";
      break;
  }

  json["value"] = ToJson(bound.value());

  return json;
}

json ToJson(const Symbol &symbol) { return symbol.name(); }

json ToJson(storage::EdgeTypeId edge_type, const DbAccessor &dba) { return dba.EdgeTypeToName(edge_type); }

json ToJson(storage::LabelId label, const DbAccessor &dba) { return dba.LabelToName(label); }

json ToJson(storage::PropertyId property, const DbAccessor &dba) { return dba.PropertyToName(property); }

json ToJson(NamedExpression *nexpr) {
  json json;
  json["expression"] = ToJson(nexpr->expression_);
  json["name"] = nexpr->name_;
  return json;
}

json ToJson(const std::vector<std::pair<storage::PropertyId, Expression *>> &properties, const DbAccessor &dba) {
  json json;
  for (const auto &prop_pair : properties) {
    json.emplace(ToJson(prop_pair.first, dba), ToJson(prop_pair.second));
  }
  return json;
}

json ToJson(const NodeCreationInfo &node_info, const DbAccessor &dba) {
  json self;
  self["symbol"] = ToJson(node_info.symbol);
  self["labels"] = ToJson(node_info.labels, dba);
  const auto *props = std::get_if<PropertiesMapList>(&node_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  return self;
}

json ToJson(const EdgeCreationInfo &edge_info, const DbAccessor &dba) {
  json self;
  self["symbol"] = ToJson(edge_info.symbol);
  const auto *props = std::get_if<PropertiesMapList>(&edge_info.properties);
  self["properties"] = ToJson(props ? *props : PropertiesMapList{}, dba);
  self["edge_type"] = ToJson(edge_info.edge_type, dba);
  self["direction"] = ToString(edge_info.direction);
  return self;
}

json ToJson(const Aggregate::Element &elem) {
  json json;
  if (elem.value) {
    json["value"] = ToJson(elem.value);
  }
  if (elem.key) {
    json["key"] = ToJson(elem.key);
  }
  json["op"] = utils::ToLowerCase(Aggregation::OpToString(elem.op));
  json["output_symbol"] = ToJson(elem.output_sym);
  json["distinct"] = elem.distinct;

  return json;
}
////////////////////////// END HELPER FUNCTIONS ////////////////////////////////

}  // namespace impl

}  // namespace memgraph::query::plan
