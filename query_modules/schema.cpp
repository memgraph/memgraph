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
#include "utils/string.hpp"

#include <optional>

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

void InsertRecordForLabelIndex(const auto &record_factory, const std::string_view label, std::string status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), "");
  record.Insert(std::string(Schema::kReturnKeys).c_str(), mgp::List());
  record.Insert(std::string(Schema::kReturnUnique).c_str(), false);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void InsertRecordForUniqueConstraint(const auto &record_factory, const std::string_view label,
                                     const mgp::List &properties, std::string status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), properties.ToString());
  record.Insert(std::string(Schema::kReturnKeys).c_str(), properties);
  record.Insert(std::string(Schema::kReturnUnique).c_str(), true);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void InsertRecordForLabelPropertyIndexAndExistenceConstraint(const auto &record_factory, const std::string_view label,
                                                             const std::string_view property, std::string status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), property);
  record.Insert(std::string(Schema::kReturnKeys).c_str(), mgp::List({mgp::Value(property)}));
  record.Insert(std::string(Schema::kReturnUnique).c_str(), false);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void ProcessCreatingLabelIndex(const std::string_view label,
                               const std::vector<std::string_view> &existing_label_indices, mgp_graph *memgraph_graph,
                               const auto &record_factory) {
  bool label_index_exists =
      std::find(existing_label_indices.begin(), existing_label_indices.end(), label) != existing_label_indices.end();
  if (label_index_exists) {
    InsertRecordForLabelIndex(record_factory, label, "Kept");
  } else if (mgp::CreateLabelIndexImpl(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, "Created");
  }
}

void ProcessCreatingLabelPropertyIndexAndExistenceConstraint(const std::string_view label,
                                                             const std::string_view property,
                                                             std::vector<std::string_view> &existing_collection,
                                                             const auto &func_creation, mgp_graph *memgraph_graph,
                                                             const auto &record_factory) {
  auto label_property_search_key = std::string(label) + ":" + std::string(property);
  bool label_property_index_exists = std::find(existing_collection.begin(), existing_collection.end(),
                                               label_property_search_key) != existing_collection.end();
  if (label_property_index_exists) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Kept");
    existing_collection.erase(
        std::remove(existing_collection.begin(), existing_collection.end(), label_property_search_key),
        existing_collection.end());
  } else if (func_creation(memgraph_graph, label, property)) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Created");
  }
}

using AssertedLabelIndices = std::vector<std::string_view>;
using AssertedLabelPropertyIndices = std::vector<std::pair<std::string_view, std::string_view>>;
using AssertedIndices = std::pair<AssertedLabelIndices, AssertedLabelPropertyIndices>;

AssertedIndices CreateIndicesForLabel(const std::string_view label, const mgp::Value &properties_val,
                                      mgp_graph *memgraph_graph, const auto &record_factory,
                                      std::vector<std::string_view> &existing_label_indices,
                                      std::vector<std::string_view> &existing_label_property_indices) {
  AssertedLabelIndices asserted_label_indices;
  AssertedLabelPropertyIndices asserted_label_property_indices;
  if (!properties_val.IsList()) {
    return {};
  }
  const auto properties = properties_val.ValueList();
  if (properties.Empty() && mgp::CreateLabelIndexImpl(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, "Created");
  } else {
    for (const auto &property : properties) {
      if (!property.IsString()) {
        continue;
      }
      const auto property_str = property.ValueString();
      if (property_str.empty()) {
        ProcessCreatingLabelIndex(label, existing_label_indices, memgraph_graph, record_factory);
        asserted_label_indices.emplace_back(label);
      } else {
        asserted_label_property_indices.emplace_back(label, property_str);
        ProcessCreatingLabelPropertyIndexAndExistenceConstraint(label, property_str, existing_label_property_indices,
                                                                mgp::CreateLabelPropertyIndexImpl, memgraph_graph,
                                                                record_factory);
      }
    }
  }
  return std::make_pair(asserted_label_indices, asserted_label_property_indices);
}

void ProcessIndices(const mgp::Map &indices_map, mgp_graph *memgraph_graph, const auto &record_factory,
                    bool drop_existing) {
  auto mgp_existing_label_indices = mgp::ListAllLabelIndicesImpl(memgraph_graph);
  auto mgp_existing_label_property_indices = mgp::ListAllLabelPropertyIndicesImpl(memgraph_graph);
  std::vector<std::string_view> existing_label_indices;
  std::vector<std::string_view> existing_label_property_indices;
  AssertedIndices asserted_indices;

  // TODO: (andi) You can improve it by doing this in ListAllLabelIndicesImpl
  std::transform(mgp_existing_label_indices.begin(), mgp_existing_label_indices.end(),
                 std::back_inserter(existing_label_indices), [](const auto &index) { return index.ValueString(); });
  // TODO: (andi) You can improve it by doing this in ListAllLabelPropertyIndicesImpl
  std::transform(mgp_existing_label_property_indices.begin(), mgp_existing_label_property_indices.end(),
                 std::back_inserter(existing_label_property_indices),
                 [](const auto &index) { return index.ValueString(); });

  for (const auto &[label, properties_val] : indices_map) {
    AssertedIndices asserted_indices_new = CreateIndicesForLabel(
        label, properties_val, memgraph_graph, record_factory, existing_label_indices, existing_label_property_indices);
    if (drop_existing) {
      asserted_indices.first.insert(asserted_indices.first.end(), asserted_indices_new.first.begin(),
                                    asserted_indices_new.first.end());
      asserted_indices.second.insert(asserted_indices.second.end(), asserted_indices_new.second.begin(),
                                     asserted_indices_new.second.end());
    }
  }

  if (!drop_existing) {
    return;
  }

  std::vector<std::string_view> label_indices_to_drop;
  std::vector<std::string_view> label_property_indices_to_drop;

  std::copy_if(existing_label_indices.begin(), existing_label_indices.end(), std::back_inserter(label_indices_to_drop),
               [&](const auto &label) {
                 return std::find(asserted_indices.first.begin(), asserted_indices.first.end(), label) ==
                        asserted_indices.first.end();
               });

  std::copy_if(existing_label_property_indices.begin(), existing_label_property_indices.end(),
               std::back_inserter(label_property_indices_to_drop), [&](const auto &label_property) {
                 const auto label_size = label_property.find(':');
                 const auto label = label_property.substr(0, label_size);
                 const auto property = label_property.substr(label_size + 1);

                 const auto search_pair = std::make_pair(label, property);

                 return std::find(asserted_indices.second.begin(), asserted_indices.second.end(), search_pair) ==
                        asserted_indices.second.end();
               });

  std::for_each(label_indices_to_drop.begin(), label_indices_to_drop.end(), [&](const std::string_view label) {
    if (mgp::DropLabelIndexImpl(memgraph_graph, label)) {
      InsertRecordForLabelIndex(record_factory, label, "Dropped");
    }
  });

  std::for_each(label_property_indices_to_drop.begin(), label_property_indices_to_drop.end(),
                [&](const std::string_view label_property) {
                  const auto label_size = label_property.find(':');
                  const auto label = std::string(label_property.substr(0, label_size));
                  const auto property = std::string(label_property.substr(label_size + 1));
                  if (mgp::DropLabelPropertyIndexImpl(memgraph_graph, label, property)) {
                    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Dropped");
                  }
                });
}

using ExistenceConstraintsStorage = std::vector<std::pair<std::string_view, std::string_view>>;

ExistenceConstraintsStorage CreateExistenceConstraintsForLabel(
    const std::string_view label, const mgp::Value &properties_val, mgp_graph *memgraph_graph,
    const auto &record_factory, std::vector<std::string_view> &existing_existence_constraints) {
  ExistenceConstraintsStorage asserted_existence_constraints;
  if (!properties_val.IsList()) {
    return asserted_existence_constraints;
  }

  const auto &properties = properties_val.ValueList();
  for (const auto &property : properties) {
    if (!property.IsString()) {
      continue;
    }
    const auto property_str = property.ValueString();
    if (property_str.empty()) {
      continue;
    }
    asserted_existence_constraints.emplace_back(label, property_str);
    ProcessCreatingLabelPropertyIndexAndExistenceConstraint(label, property_str, existing_existence_constraints,
                                                            mgp::CreateExistenceConstraintImpl, memgraph_graph,
                                                            record_factory);
  }
  return asserted_existence_constraints;
}

void ProcessExistenceConstraints(const mgp::Map &existence_constraints_map, mgp_graph *memgraph_graph,
                                 const auto &record_factory, bool drop_existing) {
  auto mgp_existing_existence_constraints = mgp::ListAllExistenceConstraintsImpl(memgraph_graph);
  // TODO: (andi) pair instead of string processing
  std::vector<std::string_view> existing_existence_constraints;
  std::transform(mgp_existing_existence_constraints.begin(), mgp_existing_existence_constraints.end(),
                 std::back_inserter(existing_existence_constraints),
                 [](const auto &constraint) { return constraint.ValueString(); });

  ExistenceConstraintsStorage asserted_existence_constraints;

  for (const auto &existing_constraint : existence_constraints_map) {
    auto asserted_existence_constraints_new =
        CreateExistenceConstraintsForLabel(existing_constraint.key, existing_constraint.value, memgraph_graph,
                                           record_factory, existing_existence_constraints);
    if (drop_existing) {
      asserted_existence_constraints.insert(asserted_existence_constraints.end(),
                                            asserted_existence_constraints_new.begin(),
                                            asserted_existence_constraints_new.end());
    }
  }

  if (!drop_existing) {
    return;
  }

  std::vector<std::string_view> existence_constraints_to_drop;

  std::copy_if(existing_existence_constraints.begin(), existing_existence_constraints.end(),
               std::back_inserter(existence_constraints_to_drop), [&](const auto &label_property) {
                 const auto label_size = label_property.find(':');
                 const auto label = label_property.substr(0, label_size);
                 const auto property = label_property.substr(label_size + 1);
                 const auto search_pair = std::make_pair(label, property);

                 return std::find(asserted_existence_constraints.begin(), asserted_existence_constraints.end(),
                                  search_pair) == asserted_existence_constraints.end();
               });

  std::for_each(existence_constraints_to_drop.begin(), existence_constraints_to_drop.end(),
                [&](const auto &label_property) {
                  const auto label_size = label_property.find(':');
                  const auto label = std::string(label_property.substr(0, label_size));
                  const auto property = std::string(label_property.substr(label_size + 1));
                  if (mgp::DropExistenceConstraintImpl(memgraph_graph, label, property)) {
                    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Dropped");
                  }
                });
}

void CreateUniqueConstraintsForLabel(const std::string_view label, const mgp::Value &unique_props_nested,
                                     std::vector<std::string_view> &existing_unique_constraints,
                                     mgp_graph *memgraph_graph, const auto &record_factory) {
  if (!unique_props_nested.IsList()) {
    return;
  }
  const auto unique_props_nested_list = unique_props_nested.ValueList();
  for (const auto &properties : unique_props_nested_list) {
    if (!properties.IsList()) {
      continue;
    }
    auto properties_list = properties.ValueList();
    bool unique_constraint_should_be_created = true;
    std::string properties_str;
    for (const auto &property : properties_list) {
      properties_str += std::string(property.ValueString()) + ",";
      if (!property.IsString() || property.ValueString().empty()) {
        unique_constraint_should_be_created = false;
        break;
      }
    }
    if (!unique_constraint_should_be_created) {
      continue;
    }

    auto constraint_key = std::string(label) + ":" + properties_str;
    bool constraint_exists = std::find(existing_unique_constraints.begin(), existing_unique_constraints.end(),
                                       constraint_key) != existing_unique_constraints.end();

    if (constraint_exists) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, "Kept");
      existing_unique_constraints.erase(
          std::remove(existing_unique_constraints.begin(), existing_unique_constraints.end(), constraint_key),
          existing_unique_constraints.end());

    } else if (mgp::CreateUniqueConstraintImpl(memgraph_graph, label, properties.ptr())) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, "Created");
    }
  }
}

void ProcessUniqueConstraints(const mgp::Map &unique_constraints_map, mgp_graph *memgraph_graph,
                              const auto &record_factory) {
  auto mgp_existing_unique_constraints = mgp::ListAllUniqueConstraintsImpl(memgraph_graph);
  std::vector<std::string_view> existing_unique_constraints;
  std::transform(mgp_existing_unique_constraints.begin(), mgp_existing_unique_constraints.end(),
                 std::back_inserter(existing_unique_constraints),
                 [](const auto &constraint) { return constraint.ValueString(); });
  for (const auto &[label, unique_props_nested] : unique_constraints_map) {
    CreateUniqueConstraintsForLabel(label, unique_props_nested, existing_unique_constraints, memgraph_graph,
                                    record_factory);
  }
}

void Schema::Assert(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);
  auto indices_map = arguments[0].ValueMap();
  auto unique_constraints_map = arguments[1].ValueMap();
  auto existence_constraints_map = arguments[2].ValueMap();
  auto drop_existing = arguments[3].ValueBool();

  ProcessIndices(indices_map, memgraph_graph, record_factory, drop_existing);
  ProcessExistenceConstraints(existence_constraints_map, memgraph_graph, record_factory, drop_existing);
  ProcessUniqueConstraints(unique_constraints_map, memgraph_graph, record_factory);
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
