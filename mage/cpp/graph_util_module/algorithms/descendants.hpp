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

#include <list>

#include <mgp.hpp>

const char *kResultDescendants = "descendants";

void Descendants(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto given_node = arguments[0].ValueNode();

    mgp::List result = mgp::List();

    std::set<uint64_t> visited_node_ids;
    std::list<mgp::Node> next_nodes;

    for (const auto relationship : given_node.OutRelationships()) {
      const auto target_node = relationship.To();

      if (visited_node_ids.find(target_node.Id().AsUint()) == visited_node_ids.end()) {
        visited_node_ids.insert(target_node.Id().AsUint());
        next_nodes.emplace_back(target_node);
      }
    }

    while (next_nodes.size()) {
      const auto current_node = next_nodes.front();
      result.AppendExtend(mgp::Value(current_node));
      next_nodes.pop_front();

      for (const auto relationship : current_node.OutRelationships()) {
        const auto target_node = relationship.To();

        if (visited_node_ids.find(target_node.Id().AsUint()) == visited_node_ids.end()) {
          visited_node_ids.insert(target_node.Id().AsUint());
          next_nodes.emplace_back(target_node);
        }
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(kResultDescendants, result);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
