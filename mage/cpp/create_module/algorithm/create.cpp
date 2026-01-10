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

#include "create.hpp"

namespace {
void ThrowException(const mgp::Value &value) {
  std::ostringstream oss;
  oss << value.Type();
  throw mgp::ValueException("Unsupported type for this operation, received type: " + oss.str());
}

void DeleteAndOutput(mgp::Relationship &relationship, const mgp::List &keys, const mgp::RecordFactory &record_factory) {
  for (const auto &key : keys) {
    relationship.RemoveProperty(std::string(key.ValueString()));
  }

  auto record = record_factory.NewRecord();
  record.Insert(std::string(Create::kResultRemoveRelProperties).c_str(), relationship);
}

void ModifyAndOutput(mgp::Relationship &relationship, const mgp::List &keys, const mgp::List &values,
                     const mgp::RecordFactory &record_factory) {
  auto it1 = keys.begin();
  auto it2 = values.begin();

  while (it1 != keys.end() && it2 != values.end()) {
    relationship.SetProperty(std::string((*it1).ValueString()), *it2);
    ++it1;
    ++it2;
  }

  auto record = record_factory.NewRecord();
  record.Insert(std::string(Create::kResultSetRelProperties).c_str(), relationship);
}

}  // namespace

void Create::RemoveRelProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const mgp::Graph graph{memgraph_graph};
    const auto keys{arguments[1].ValueList()};

    std::unordered_set<int64_t> ids;

    auto ProcessValue = [&](const mgp::Value &value) {
      if (value.IsRelationship()) {
        auto relationship_copy = value.ValueRelationship();
        DeleteAndOutput(relationship_copy, keys, record_factory);
      } else if (value.IsInt()) {
        ids.insert(value.ValueInt());
      } else {
        ThrowException(value);
      }
    };

    if (!arguments[0].IsList()) {
      ProcessValue(arguments[0]);
    } else {
      for (const auto &list_item : arguments[0].ValueList()) {
        ProcessValue(list_item);
      }
    }

    if (ids.empty()) {
      return;
    }

    for (auto relationship : graph.Relationships()) {
      if (ids.contains(relationship.Id().AsInt())) {
        DeleteAndOutput(relationship, keys, record_factory);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::SetRelProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const mgp::Graph graph{memgraph_graph};
    const auto keys{arguments[1].ValueList()};
    const auto values{arguments[2].ValueList()};

    if (keys.Size() != values.Size()) {
      throw mgp::IndexException("Keys and values are not the same size");
    }

    std::unordered_set<int64_t> ids;

    auto ProcessValue = [&](const mgp::Value &value) {
      if (value.IsRelationship()) {
        auto relationship_copy = value.ValueRelationship();
        ModifyAndOutput(relationship_copy, keys, values, record_factory);
      } else if (value.IsInt()) {
        ids.insert(value.ValueInt());
      } else {
        ThrowException(value);
      }
    };

    if (!arguments[0].IsList()) {
      ProcessValue(arguments[0]);
    } else {
      for (const auto &list_item : arguments[0].ValueList()) {
        ProcessValue(list_item);
      }
    }

    if (ids.empty()) {
      return;
    }

    for (auto relationship : graph.Relationships()) {
      if (ids.contains(relationship.Id().AsInt())) {
        ModifyAndOutput(relationship, keys, values, record_factory);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::Relationship(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    auto node_from{arguments[0].ValueNode()};
    const auto relationship_type{arguments[1].ValueString()};
    const auto properties{arguments[2].ValueMap()};
    auto node_to{arguments[3].ValueNode()};

    mgp::Graph graph{memgraph_graph};
    auto relationship = graph.CreateRelationship(node_from, node_to, relationship_type);

    for (const auto &map_item : properties) {
      relationship.SetProperty(std::string(map_item.key), map_item.value);
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultRelationship).c_str(), relationship);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::SetElementProp(mgp::Relationship &element, const mgp::List &prop_key_list,
                            const mgp::List &prop_value_list, const mgp::RecordFactory &record_factory) {
  for (size_t i = 0; i < prop_key_list.Size(); i++) {
    element.SetProperty(std::string(prop_key_list[i].ValueString()), prop_value_list[i]);
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultRelProp).c_str(), element);
  }
}

void Create::ProcessElement(const mgp::Value &element, const mgp::Graph graph [[maybe_unused]],
                            const mgp::List &prop_key_list, const mgp::List &prop_value_list,
                            const mgp::RecordFactory &record_factory, std::unordered_set<mgp::Id> &relIds) {
  if (!(element.IsRelationship() || element.IsInt())) {
    throw mgp::ValueException("First argument must be a relationship, id or a list of those.");
  }
  if (element.IsRelationship()) {
    auto rel = element.ValueRelationship();
    SetElementProp(rel, prop_key_list, prop_value_list, record_factory);
    return;
  }
  relIds.insert(mgp::Id::FromInt(element.ValueInt()));
}

void Create::SetRelProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    mgp::List prop_key_list = mgp::List();
    prop_key_list.AppendExtend(arguments[1]);
    mgp::List prop_value_list = mgp::List();
    prop_value_list.AppendExtend(arguments[2]);

    std::unordered_set<mgp::Id> relIds;

    if (arguments[0].IsList()) {
      for (const auto element : arguments[0].ValueList()) {
        ProcessElement(element, graph, prop_key_list, prop_value_list, record_factory, relIds);
      }
    } else {
      ProcessElement(arguments[0], graph, prop_key_list, prop_value_list, record_factory, relIds);
    }
    if (relIds.empty()) {
      return;
    }
    for (auto rel : graph.Relationships()) {
      if (relIds.contains(rel.Id())) {
        SetElementProp(rel, prop_key_list, prop_value_list, record_factory);
      }
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::RemoveElementProperties(mgp::Node &element, const mgp::List &properties_keys,
                                     const mgp::RecordFactory &record_factory) {
  for (auto key : properties_keys) {
    const std::string key_str(key.ValueString());
    element.RemoveProperty(key_str);
  }
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Create::kReturntRemoveProperties).c_str(), element);
}

void Create::RemoveElementLabels(mgp::Node &element, const mgp::List &labels,
                                 const mgp::RecordFactory &record_factory) {
  for (auto label : labels) {
    element.RemoveLabel(label.ValueString());
  }
  auto record = record_factory.NewRecord();
  record.Insert(std::string(kResultRemoveLabels).c_str(), element);
}

void Create::ProcessElement(const mgp::Value &element, const mgp::Graph graph, const mgp::List &list_keys,
                            const bool labels_or_props, const mgp::RecordFactory &record_factory) {
  if (!(element.IsNode() || element.IsInt())) {
    throw mgp::ValueException("First argument must be type node, id or a list of those.");
  }
  if (element.IsNode()) {
    auto node = element.ValueNode();
    if (labels_or_props == 0) {
      RemoveElementLabels(node, list_keys, record_factory);
    } else {
      RemoveElementProperties(node, list_keys, record_factory);
    }
    return;
  }
  auto node = graph.GetNodeById(mgp::Id::FromInt(element.ValueInt()));
  if (labels_or_props == 0) {
    RemoveElementLabels(node, list_keys, record_factory);
  } else {
    RemoveElementProperties(node, list_keys, record_factory);
  }
}

void Create::RemoveLabels(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto labels = arguments[1].ValueList();

    if (arguments[0].IsList()) {
      for (const auto element : arguments[0].ValueList()) {
        ProcessElement(element, graph, labels, false, record_factory);
      }
      return;
    }
    ProcessElement(arguments[0], graph, labels, false, record_factory);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::RemoveProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto graph = mgp::Graph(memgraph_graph);
    const auto list_keys = arguments[1].ValueList();

    if (arguments[0].IsList()) {
      for (const auto element : arguments[0].ValueList()) {
        ProcessElement(element, graph, list_keys, false, record_factory);
      }
      return;
    }
    ProcessElement(arguments[0], graph, list_keys, false, record_factory);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::SetElementProp(mgp::Node &element, const mgp::List &prop_key_list, const mgp::List &prop_value_list,
                            const mgp::RecordFactory &record_factory) {
  for (size_t i = 0; i < prop_key_list.Size(); i++) {
    element.SetProperty(std::string(prop_key_list[i].ValueString()), prop_value_list[i]);
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultProperties).c_str(), element);
  }
}

void Create::ProcessElement(const mgp::Value &element, const mgp::Graph graph, const mgp::List &prop_key_list,
                            const mgp::List &prop_value_list, const mgp::RecordFactory &record_factory) {
  if (element.IsNode()) {
    auto node = element.ValueNode();
    SetElementProp(node, prop_key_list, prop_value_list, record_factory);
    return;
  }
  if (element.IsInt()) {
    auto node = graph.GetNodeById(mgp::Id::FromInt(element.ValueInt()));
    SetElementProp(node, prop_key_list, prop_value_list, record_factory);
    return;
  }
  throw mgp::ValueException("First argument must be a node, id or a list of those.");
}

void Create::SetProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto prop_key_list = arguments[1].ValueList();
    const auto prop_value_list = arguments[2].ValueList();

    if (prop_key_list.Size() != prop_value_list.Size()) {
      throw mgp::IndexException("Key and value lists must be the same size.");
    }

    if (!arguments[0].IsList()) {
      ProcessElement(arguments[0], graph, prop_key_list, prop_value_list, record_factory);
      return;
    }
    for (const auto element : arguments[0].ValueList()) {
      ProcessElement(element, graph, prop_key_list, prop_value_list, record_factory);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto graph = mgp::Graph(memgraph_graph);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto labels = arguments[0].ValueList();
    const auto properties = arguments[1].ValueMap();

    mgp::Node node = graph.CreateNode();

    for (const auto label : labels) {
      node.AddLabel(label.ValueString());
    }

    for (const auto property : properties) {
      node.SetProperty((std::string)property.key, property.value);
    }

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kResultNode).c_str(), node);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::Nodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto graph = mgp::Graph(memgraph_graph);
    const mgp::List labels = arguments[0].ValueList();
    const mgp::List properties = arguments[1].ValueList();
    const auto num_of_nodes = static_cast<int64_t>(properties.Size());
    for (auto i = 0; i < num_of_nodes; i++) {
      mgp::Node node = graph.CreateNode();
      for (auto label : labels) {
        node.AddLabel(label.ValueString());
      }
      const mgp::Map node_properties = properties[i].ValueMap();
      for (auto item : node_properties) {
        node.SetProperty(std::string(item.key), item.value);
      }
      auto record = record_factory.NewRecord();
      record.Insert(std::string(Create::kReturnNodes).c_str(), node);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Create::SetProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto graph = mgp::Graph(memgraph_graph);
    mgp::List key;
    key.AppendExtend(arguments[1]);
    mgp::List value;
    value.AppendExtend(arguments[2]);

    if (!arguments[0].IsList()) {
      ProcessElement(arguments[0], graph, key, value, record_factory);
      return;
    }
    for (const auto element : arguments[0].ValueList()) {
      ProcessElement(element, graph, key, value, record_factory);
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
