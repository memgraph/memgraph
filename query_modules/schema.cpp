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

#include <cassert>

namespace Schema {

constexpr std::string_view kReturnNodeType = "nodeType";
constexpr std::string_view kProcedureNodeType = "node_type_properties";
constexpr std::string_view kProcedureRelType = "rel_type_properties";
constexpr std::string_view kProcedureAssert = "assert";
constexpr std::string_view kReturnLabels = "nodeLabels";
constexpr std::string_view kReturnRelType = "relType";
constexpr std::string_view kReturnPropertyName = "propertyName";
constexpr std::string_view kReturnPropertyType = "propertyTypes";
constexpr std::string_view kReturnMandatory = "mandatory";
constexpr std::string_view kReturnLabel = "label";
constexpr std::string_view kReturnKey = "key";
constexpr std::string_view kReturnKeys = "keys";
constexpr std::string_view kReturnUnique = "unique";
constexpr std::string_view kReturnAction = "action";
constexpr std::string_view kParameterIndices = "indices";
constexpr std::string_view kParameterUniqueConstraints = "unique_constraints";
constexpr std::string_view kParameterExistenceConstraints = "existence_constraints";
constexpr std::string_view kParameterDropExisting = "drop_existing";

std::string TypeOf(const mgp::Type &type);

template <typename T>
void ProcessPropertiesNode(mgp::Record &record, const std::string &type, const mgp::List &labels,
                           const std::string &propertyName, const T &propertyType, const bool &mandatory);

template <typename T>
void ProcessPropertiesRel(mgp::Record &record, const std::string_view &type, const std::string &propertyName,
                          const T &propertyType, const bool &mandatory);

void NodeTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RelTypeProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Assert(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
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

void Schema::NodeTypeProperties(mgp_list * /*args*/, mgp_graph *memgraph_graph, mgp_result *result,
                                mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  ;
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph = mgp::Graph(memgraph_graph);
    for (auto node : graph.Nodes()) {
      std::string type;
      mgp::List labels = mgp::List();
      for (auto label : node.Labels()) {
        labels.AppendExtend(mgp::Value(label));
        type += ":`" + std::string(label) + "`";
      }

      if (node.Properties().empty()) {
        auto record = record_factory.NewRecord();
        ProcessPropertiesNode<std::string>(record, type, labels, "", "", false);
        continue;
      }

      for (auto &[key, prop] : node.Properties()) {
        auto property_type = mgp::List();
        auto record = record_factory.NewRecord();
        property_type.AppendExtend(mgp::Value(TypeOf(prop.Type())));
        ProcessPropertiesNode<mgp::List>(record, type, labels, key, property_type, true);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Schema::RelTypeProperties(mgp_list * /*args*/, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph = mgp::Graph(memgraph_graph);

    for (auto rel : graph.Relationships()) {
      std::string type = ":`" + std::string(rel.Type()) + "`";
      if (rel.Properties().empty()) {
        auto record = record_factory.NewRecord();
        ProcessPropertiesRel<std::string>(record, type, "", "", false);
        continue;
      }

      for (auto &[key, prop] : rel.Properties()) {
        auto property_type = mgp::List();
        auto record = record_factory.NewRecord();
        property_type.AppendExtend(mgp::Value(TypeOf(prop.Type())));
        ProcessPropertiesRel<mgp::List>(record, type, key, property_type, true);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void CreateIndices(const mgp::Map &indices_map, mgp_graph *memgraph_graph, const auto &record_factory) {
  for (const auto &label_property : indices_map) {
    const auto label = label_property.key;
    const auto property_str = label_property.value.ValueString();
    auto success = std::invoke([memgraph_graph, label, property_str]() {
      if (property_str.empty()) {
        return mgp::CreateLabelIndexImpl(memgraph_graph, label);
      }
      return mgp::CreateLabelPropertyIndexImpl(memgraph_graph, label, property_str);
    });
    if (success) {
      auto record = record_factory.NewRecord();
      record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
      record.Insert(std::string(Schema::kReturnKey).c_str(), property_str);
      record.Insert(std::string(Schema::kReturnKeys).c_str(), mgp::List({label_property.value}));
      record.Insert(std::string(Schema::kReturnUnique).c_str(), false);
      record.Insert(std::string(Schema::kReturnAction).c_str(), "Created");
    }
  }
}

void CreateExistenceConstraints(const mgp::Map &existence_constraints_map, mgp_graph *memgraph_graph) {
  for (const auto &[label, property] : existence_constraints_map) {
    const auto property_str = property.ValueString();
    assert((!property_str.empty()) && "Property name must be provided for existence constraint");
    mgp::CreateExistenceConstraintImpl(memgraph_graph, label, property_str);
  }
}

void CreateUniqueConstraints(const mgp::Map &unique_constraints_map, mgp_graph *memgraph_graph) {
  for (const auto &[label, properties] : unique_constraints_map) {
    auto properties_list = properties.ValueList();
    std::vector<std::string_view> properties_str;
    properties_str.reserve(properties_list.Size());
    for (const auto &property : properties_list) {
      const auto property_str = property.ValueString();
      assert((!property_str.empty()) && "Property name must be provided for unique constraint");
      properties_str.push_back(property_str);
    }
    mgp::CreateUniqueConstraintImpl(memgraph_graph, label, properties_str);
  }
}

void Schema::Assert(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);
  auto indices_map = arguments[0].ValueMap();
  auto unique_constraints_map = arguments[1].ValueMap();
  auto existence_constraints_map = arguments[2].ValueMap();

  CreateIndices(indices_map, memgraph_graph, record_factory);
  CreateUniqueConstraints(unique_constraints_map, memgraph_graph);
  CreateExistenceConstraints(existence_constraints_map, memgraph_graph);

  auto record = record_factory.NewRecord();
  record.Insert(std::string(kReturnLabel).c_str(), "All");
  record.Insert(std::string(kReturnKey).c_str(), "All");
  record.Insert(std::string(kReturnKeys).c_str(), mgp::List());
  record.Insert(std::string(kReturnUnique).c_str(), false);
  record.Insert(std::string(kReturnAction).c_str(), "Created");

  // mgp::Graph ima pointer na mgp_graph
  // DropIndex(mgp_graph *memgraph_graph);
  // Take care about the lock
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    ;

    AddProcedure(Schema::NodeTypeProperties, Schema::kProcedureNodeType, mgp::ProcedureType::Read, {},
                 {mgp::Return(Schema::kReturnNodeType, mgp::Type::String),
                  mgp::Return(Schema::kReturnLabels, {mgp::Type::List, mgp::Type::String}),
                  mgp::Return(Schema::kReturnPropertyName, mgp::Type::String),
                  mgp::Return(Schema::kReturnPropertyType, mgp::Type::Any),
                  mgp::Return(Schema::kReturnMandatory, mgp::Type::Bool)},
                 module, memory);

    AddProcedure(Schema::RelTypeProperties, Schema::kProcedureRelType, mgp::ProcedureType::Read, {},
                 {mgp::Return(Schema::kReturnRelType, mgp::Type::String),
                  mgp::Return(Schema::kReturnPropertyName, mgp::Type::String),
                  mgp::Return(Schema::kReturnPropertyType, mgp::Type::Any),
                  mgp::Return(Schema::kReturnMandatory, mgp::Type::Bool)},
                 module, memory);
    AddProcedure(
        Schema::Assert, Schema::kProcedureAssert, mgp::ProcedureType::Read,
        {
            mgp::Parameter(Schema::kParameterIndices, {mgp::Type::Map, mgp::Type::Any}),
            mgp::Parameter(Schema::kParameterUniqueConstraints, {mgp::Type::Map, mgp::Type::Any}),
            mgp::Parameter(Schema::kParameterExistenceConstraints, {mgp::Type::Map, mgp::Type::Any},
                           mgp::Value(mgp::Map{})),
            mgp::Parameter(Schema::kParameterDropExisting, mgp::Type::Bool, mgp::Value(true)),
        },
        {mgp::Return(Schema::kReturnLabel, mgp::Type::String), mgp::Return(Schema::kReturnKey, mgp::Type::String),
         mgp::Return(Schema::kReturnKeys, {mgp::Type::List, mgp::Type::String}),
         mgp::Return(Schema::kReturnUnique, mgp::Type::Bool), mgp::Return(Schema::kReturnAction, mgp::Type::String)},
        module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
