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

#include <mgp.hpp>

namespace Schema {

/*NodeTypeProperties and RelTypeProperties constants*/
constexpr std::string_view kReturnNodeType = "nodeType";
constexpr std::string_view kProcedureNodeType = "node_type_properties";
constexpr std::string_view kProcedureRelType = "rel_type_properties";
constexpr std::string_view kReturnLabels = "nodeLabels";
constexpr std::string_view kReturnRelType = "relType";
constexpr std::string_view kReturnPropertyName = "propertyName";
constexpr std::string_view kReturnPropertyType = "propertyTypes";
constexpr std::string_view kReturnMandatory = "mandatory";

std::string TypeOf(const mgp::Type &type);

template <typename T>
void ProcessPropertiesNode(mgp::Record &record, const std::string &type, const mgp::List &labels,
                           const std::string &propertyName, const T &propertyType, const bool &mandatory);

template <typename T>
void ProcessPropertiesRel(mgp::Record &record, const std::string_view &type, const std::string &propertyName,
                          const T &propertyType, const bool &mandatory);

void NodeTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RelTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace Schema

/*we have << operator for type in Cpp API, but in it we return somewhat different strings than I would like in this
module, so I implemented a small function here*/
std::string Schema::TypeOf(const mgp::Type &type) {
  switch (type) {
    case mgp::Type::Null:
      return "Null";
    case mgp::Type::Bool:
      return "Bool";
    case mgp::Type::Int:
      return "Int";
    case mgp::Type::Double:
      return "Double";
    case mgp::Type::String:
      return "String";
    case mgp::Type::List:
      return "List[Any]";
    case mgp::Type::Map:
      return "Map[Any]";
    case mgp::Type::Node:
      return "Vertex";
    case mgp::Type::Relationship:
      return "Edge";
    case mgp::Type::Path:
      return "Path";
    case mgp::Type::Date:
      return "Date";
    case mgp::Type::LocalTime:
      return "LocalTime";
    case mgp::Type::LocalDateTime:
      return "LocalDateTime";
    case mgp::Type::Duration:
      return "Duration";
    default:
      throw mgp::ValueException("Unsupported type");
  }
}
template <typename T>
void Schema::ProcessPropertiesNode(mgp::Record &record, const std::string &type, const mgp::List &labels,
                                   const std::string &propertyName, const T &propertyType, const bool &mandatory) {
  record.Insert(std::string(kReturnNodeType).c_str(), type);
  record.Insert(std::string(kReturnLabels).c_str(), labels);
  record.Insert(std::string(kReturnPropertyName).c_str(), propertyName);
  record.Insert(std::string(kReturnPropertyType).c_str(), propertyType);
  record.Insert(std::string(kReturnMandatory).c_str(), mandatory);
}

template <typename T>
void Schema::ProcessPropertiesRel(mgp::Record &record, const std::string_view &type, const std::string &propertyName,
                                  const T &propertyType, const bool &mandatory) {
  record.Insert(std::string(kReturnRelType).c_str(), type);
  record.Insert(std::string(kReturnPropertyName).c_str(), propertyName);
  record.Insert(std::string(kReturnPropertyType).c_str(), propertyType);
  record.Insert(std::string(kReturnMandatory).c_str(), mandatory);
}

// Custom equality operator for vector of strings
// We need this because Label1:Label2 and Label2:Label1 are the same
struct VectorComparator {
  bool operator()(const std::vector<std::string> &lhs, const std::vector<std::string> &rhs) const {
    // Sort and compare the vectors of strings
    std::vector<std::string> sortedLhs = lhs;
    std::vector<std::string> sortedRhs = rhs;
    std::sort(sortedLhs.begin(), sortedLhs.end());
    std::sort(sortedRhs.begin(), sortedRhs.end());

    return sortedLhs < sortedRhs;
  }
};

void Schema::NodeTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  try {
    std::map<std::vector<std::string>, std::tuple<std::set<std::string>, std::string, mgp::List>, VectorComparator>
        node_types;  // map of node types, key is vector of labels, value is tuple of set of properties, type of node
                     // and list of labels
    std::unordered_map<std::string, mgp::Value> properties;
    const mgp::Graph graph = mgp::Graph(memgraph_graph);
    for (auto node : graph.Nodes()) {
      std::string type = "";
      mgp::List labels = mgp::List();
      std::vector<std::string> labels_vector = {};
      for (auto label : node.Labels()) {
        labels.AppendExtend(mgp::Value(label));
        type += ":`" + std::string(label) + "`";
        labels_vector.emplace_back(std::string(label));
      }

      auto it = node_types.find(labels_vector);
      if (it == node_types.end()) {
        node_types[labels_vector] = std::make_tuple(std::set<std::string>({""}), type, labels);
      }

      if (node.Properties().size() == 0) {
        continue;
      }

      for (auto &[key, prop] : node.Properties()) {
        auto tuple = node_types.at(labels_vector);
        std::get<0>(tuple).insert(key);
        std::get<0>(tuple).erase("");
        node_types[labels_vector] = tuple;
        properties[key] = prop;
      }
    }

    for (auto &[labels, props] : node_types) {
      for (auto prop : std::get<0>(props)) {
        auto record = record_factory.NewRecord();
        if (prop == "") {
          ProcessPropertiesNode<std::string>(record, std::get<1>(props), std::get<2>(props), "", "", false);
          continue;
        }
        auto property_type = mgp::List();
        property_type.AppendExtend(mgp::Value(TypeOf(properties[prop].Type())));
        ProcessPropertiesNode<mgp::List>(record, std::get<1>(props), std::get<2>(props), prop, property_type, true);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Schema::RelTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  std::map<std::string, std::tuple<std::set<std::string>, std::string>> rel_types;
  std::unordered_map<std::string, mgp::Value> properties;
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph = mgp::Graph(memgraph_graph);
    for (auto rel : graph.Relationships()) {
      std::string type = ":`" + std::string(rel.Type()) + "`";
      auto it = rel_types.find(type);
      if (it == rel_types.end()) {
        rel_types[type] = std::make_tuple(std::set<std::string>({""}), type);
      }

      if (rel.Properties().size() == 0) {
        continue;
      }

      for (auto &[key, prop] : rel.Properties()) {
        auto tuple = rel_types.at(type);
        std::get<0>(tuple).insert(key);
        std::get<0>(tuple).erase("");
        rel_types[type] = tuple;
        properties[key] = prop;
      }
    }

    for (auto &[type, props] : rel_types) {
      for (auto prop : std::get<0>(props)) {
        auto record = record_factory.NewRecord();
        if (prop == "") {
          ProcessPropertiesRel<std::string>(record, std::get<1>(props), "", "", false);
          continue;
        }
        auto property_type = mgp::List();
        property_type.AppendExtend(mgp::Value(TypeOf(properties[prop].Type())));
        ProcessPropertiesRel<mgp::List>(record, std::get<1>(props), prop, property_type, true);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    ;

    AddProcedure(Schema::NodeTypeProperties, std::string(Schema::kProcedureNodeType).c_str(), mgp::ProcedureType::Read,
                 {},
                 {mgp::Return(std::string(Schema::kReturnNodeType).c_str(), mgp::Type::String),
                  mgp::Return(std::string(Schema::kReturnLabels).c_str(), {mgp::Type::List, mgp::Type::String}),
                  mgp::Return(std::string(Schema::kReturnPropertyName).c_str(), mgp::Type::String),
                  mgp::Return(std::string(Schema::kReturnPropertyType).c_str(), mgp::Type::Any),
                  mgp::Return(std::string(Schema::kReturnMandatory).c_str(), mgp::Type::Bool)},
                 module, memory);

    AddProcedure(Schema::RelTypeProperties, std::string(Schema::kProcedureRelType).c_str(), mgp::ProcedureType::Read,
                 {},
                 {mgp::Return(std::string(Schema::kReturnRelType).c_str(), mgp::Type::String),
                  mgp::Return(std::string(Schema::kReturnPropertyName).c_str(), mgp::Type::String),
                  mgp::Return(std::string(Schema::kReturnPropertyType).c_str(), mgp::Type::Any),
                  mgp::Return(std::string(Schema::kReturnMandatory).c_str(), mgp::Type::Bool)},
                 module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
