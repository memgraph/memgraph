// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mgp.hpp>

const char *kResultConnections = "connections";

void ConnectNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto list_of_nodes = arguments[0].ValueList();

    mgp::List result = mgp::List();

    std::set<uint64_t> graph_nodes;
    for (const auto node_element : list_of_nodes) {
      const auto node = node_element.ValueNode();
      graph_nodes.insert(node.Id().AsUint());
    }

    for (const auto node_element : list_of_nodes) {
      const auto node = node_element.ValueNode();
      for (const auto relationship : node.OutRelationships()) {
        const auto in_node_id = relationship.To().Id().AsUint();
        const auto out_node_id = node.Id().AsUint();

        if (graph_nodes.find(in_node_id) != graph_nodes.end() && graph_nodes.find(out_node_id) != graph_nodes.end()) {
          result.AppendExtend(mgp::Value(relationship));
        }
      }
    }

    auto record = record_factory.NewRecord();
    record.Insert(kResultConnections, result);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
