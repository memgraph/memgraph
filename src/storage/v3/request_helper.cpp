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

#include "storage/v3/request_helper.hpp"

#include <vector>

#include "pretty_print_ast_to_original_expression.hpp"
#include "storage/v3/bindings/db_accessor.hpp"
#include "storage/v3/expr.hpp"

namespace memgraph::storage::v3 {

std::vector<Element> OrderByElements(Shard::Accessor &acc, DbAccessor &dba, VerticesIterable &vertices_iterable,
                                     std::vector<msgs::OrderBy> &order_bys) {
  std::vector<Element> ordered;
  ordered.reserve(acc.ApproximateVertexCount());
  std::vector<Ordering> ordering;
  ordering.reserve(order_bys.size());
  for (const auto &order : order_bys) {
    switch (order.direction) {
      case memgraph::msgs::OrderingDirection::ASCENDING: {
        ordering.push_back(Ordering::ASC);
        break;
      }
      case memgraph::msgs::OrderingDirection::DESCENDING: {
        ordering.push_back(Ordering::DESC);
        break;
      }
    }
  }
  auto el_comparo = TypedValueVectorCompare(ordering);
  auto it = vertices_iterable.begin();
  for (; it != vertices_iterable.end(); ++it) {
    std::vector<TypedValue> properties_order_by;
    properties_order_by.reserve(order_bys.size());

    for (const auto &order_by : order_bys) {
      const auto val =
          ComputeExpression(dba, *it, std::nullopt, order_by.expression.expression, expr::identifier_node_symbol, "");
      properties_order_by.push_back(val);
    }
    ordered.push_back({std::move(properties_order_by), it});
  }

  std::sort(ordered.begin(), ordered.end(), [el_comparo](const auto &pair1, const auto &pair2) {
    return el_comparo(pair1.properties_order_by, pair2.properties_order_by);
  });
  return ordered;
}

VerticesIterable::Iterator GetStartVertexIterator(VerticesIterable &vertex_iterable,
                                                  const std::vector<PropertyValue> &start_ids, const View view) {
  auto it = vertex_iterable.begin();
  while (it != vertex_iterable.end()) {
    if (const auto &vertex = *it; start_ids <= vertex.PrimaryKey(view).GetValue()) {
      break;
    }
    ++it;
  }
  return it;
}

std::vector<Element>::const_iterator GetStartOrderedElementsIterator(const std::vector<Element> &ordered_elements,
                                                                     const std::vector<PropertyValue> &start_ids,
                                                                     const View view) {
  for (auto it = ordered_elements.begin(); it != ordered_elements.end(); ++it) {
    if (const auto &vertex = *it->vertex_it; start_ids <= vertex.PrimaryKey(view).GetValue()) {
      return it;
    }
  }
  return ordered_elements.end();
}

}  // namespace memgraph::storage::v3
