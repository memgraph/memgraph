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

std::vector<Element<VertexAccessor>>::const_iterator GetStartOrderedElementsIterator(
    const std::vector<Element<VertexAccessor>> &ordered_elements, const std::vector<PropertyValue> &start_ids,
    const View view) {
  for (auto it = ordered_elements.begin(); it != ordered_elements.end(); ++it) {
    if (const auto &vertex = it->object_acc; start_ids <= vertex.PrimaryKey(view).GetValue()) {
      return it;
    }
  }
  return ordered_elements.end();
}

std::array<std::vector<EdgeAccessor>, 2> GetEdgesFromVertex(const VertexAccessor &vertex_accessor,
                                                            const msgs::EdgeDirection direction) {
  std::vector<EdgeAccessor> in_edges;
  std::vector<EdgeAccessor> out_edges;

  switch (direction) {
    case memgraph::msgs::EdgeDirection::IN: {
      auto edges = vertex_accessor.InEdges(View::OLD);
      if (edges.HasValue()) {
        in_edges = edges.GetValue();
      }
    }
    case memgraph::msgs::EdgeDirection::OUT: {
      auto edges = vertex_accessor.OutEdges(View::OLD);
      if (edges.HasValue()) {
        out_edges = edges.GetValue();
      }
    }
    case memgraph::msgs::EdgeDirection::BOTH: {
      auto maybe_in_edges = vertex_accessor.InEdges(View::OLD);
      auto maybe_out_edges = vertex_accessor.OutEdges(View::OLD);
      std::vector<EdgeAccessor> edges;
      if (maybe_in_edges.HasValue()) {
        in_edges = maybe_in_edges.GetValue();
      }
      if (maybe_out_edges.HasValue()) {
        out_edges = maybe_out_edges.GetValue();
      }
    }
  }

  return std::array<std::vector<EdgeAccessor>, 2>{std::move(in_edges), std::move(out_edges)};
}

}  // namespace memgraph::storage::v3
