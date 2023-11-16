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

void ProcessCreatingLabelIndex(const std::string_view label, const std::set<std::string_view> &existing_label_indices,
                               mgp_graph *memgraph_graph, const auto &record_factory) {
  if (existing_label_indices.find(label) != existing_label_indices.end()) {
    InsertRecordForLabelIndex(record_factory, label, "Kept");
  } else if (mgp::CreateLabelIndexImpl(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, "Created");
  }
}

void ProcessCreatingLabelPropertyIndexAndExistenceConstraint(const std::string_view label,
                                                             const std::string_view property,
                                                             const std::set<std::string_view> &existing_collection,
                                                             const auto &func_creation, mgp_graph *memgraph_graph,
                                                             const auto &record_factory) {
  auto label_property_search_key = std::string(label) + ":" + std::string(property);
  if (existing_collection.find(label_property_search_key) != existing_collection.end()) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Kept");
  } else if (func_creation(memgraph_graph, label, property)) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Created");
  }
}

/// We collect properties for which index was created.
using AssertedIndices = std::set<std::string>;
AssertedIndices CreateIndicesForLabel(const std::string_view label, const mgp::Value &properties_val,
                                      mgp_graph *memgraph_graph, const auto &record_factory,
                                      const std::set<std::string_view> &existing_label_indices,
                                      const std::set<std::string_view> &existing_label_property_indices) {
  AssertedIndices asserted_indices;
  if (!properties_val.IsList()) {
    return {};
  }
  const auto properties = properties_val.ValueList();
  if (properties.Empty() && mgp::CreateLabelIndexImpl(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, "Created");
    asserted_indices.emplace("");
  } else {
    std::for_each(properties.begin(), properties.end(),
                  [&label, &existing_label_indices, &existing_label_property_indices, &memgraph_graph, &record_factory,
                   &asserted_indices](const mgp::Value &property) {
                    if (!property.IsString()) {
                      return;
                    }
                    const auto property_str = property.ValueString();
                    if (property_str.empty()) {
                      ProcessCreatingLabelIndex(label, existing_label_indices, memgraph_graph, record_factory);
                      asserted_indices.emplace("");
                    } else {
                      ProcessCreatingLabelPropertyIndexAndExistenceConstraint(
                          label, property_str, existing_label_property_indices, mgp::CreateLabelPropertyIndexImpl,
                          memgraph_graph, record_factory);
                      asserted_indices.emplace(property_str);
                    }
                  });
  }
  return asserted_indices;
}

void ProcessIndices(const mgp::Map &indices_map, mgp_graph *memgraph_graph, const auto &record_factory,
                    bool drop_existing) {
  auto mgp_existing_label_indices = mgp::ListAllLabelIndicesImpl(memgraph_graph);
  auto mgp_existing_label_property_indices = mgp::ListAllLabelPropertyIndicesImpl(memgraph_graph);

  std::set<std::string_view> existing_label_indices;
  std::transform(mgp_existing_label_indices.begin(), mgp_existing_label_indices.end(),
                 std::inserter(existing_label_indices, existing_label_indices.begin()),
                 [](const mgp::Value &index) { return index.ValueString(); });

  std::set<std::string_view> existing_label_property_indices;
  std::transform(mgp_existing_label_property_indices.begin(), mgp_existing_label_property_indices.end(),
                 std::inserter(existing_label_property_indices, existing_label_property_indices.begin()),
                 [](const mgp::Value &index) { return index.ValueString(); });

  std::set<std::string> asserted_label_indices;
  std::set<std::string> asserted_label_property_indices;

  auto merge_label_property = [](const std::string &label, const std::string &property) -> std::string {
    return label + ":" + property;
  };

  for (const auto &index : indices_map) {
    const std::string_view label = index.key;
    const mgp::Value &properties_val = index.value;

    AssertedIndices asserted_indices_new = CreateIndicesForLabel(
        label, properties_val, memgraph_graph, record_factory, existing_label_indices, existing_label_property_indices);

    if (!drop_existing) {
      continue;
    }
    std::for_each(asserted_indices_new.begin(), asserted_indices_new.end(),
                  [&asserted_label_indices, &asserted_label_property_indices, label,
                   &merge_label_property](const std::string &property) {
                    if (property.empty()) {
                      asserted_label_indices.emplace(label);
                    } else {
                      asserted_label_property_indices.emplace(merge_label_property(std::string(label), property));
                    }
                  });
  }

  if (!drop_existing) {
    return;
  }

  std::set<std::string_view> label_indices_to_drop;
  std::set_difference(existing_label_indices.begin(), existing_label_indices.end(), asserted_label_indices.begin(),
                      asserted_label_indices.end(),
                      std::inserter(label_indices_to_drop, label_indices_to_drop.begin()));

  std::for_each(label_indices_to_drop.begin(), label_indices_to_drop.end(),
                [memgraph_graph, record_factory](const std::string_view label) {
                  if (mgp::DropLabelIndexImpl(memgraph_graph, label)) {
                    InsertRecordForLabelIndex(record_factory, label, "Dropped");
                  }
                });

  std::set<std::string_view> label_property_indices_to_drop;
  std::set_difference(existing_label_property_indices.begin(), existing_label_property_indices.end(),
                      asserted_label_property_indices.begin(), asserted_label_property_indices.end(),
                      std::inserter(label_property_indices_to_drop, label_property_indices_to_drop.begin()));

  auto decouple_label_property = [](const std::string_view label_property) -> std::pair<std::string, std::string> {
    const auto label_size = label_property.find(':');
    const auto label = std::string(label_property.substr(0, label_size));
    const auto property = std::string(label_property.substr(label_size + 1));
    return std::make_pair(label, property);
  };

  std::for_each(label_property_indices_to_drop.begin(), label_property_indices_to_drop.end(),
                [memgraph_graph, record_factory, decouple_label_property](const std::string_view label_property) {
                  const auto [label, property] = decouple_label_property(label_property);
                  if (mgp::DropLabelPropertyIndexImpl(memgraph_graph, label, property)) {
                    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Dropped");
                  }
                });
}

using ExistenceConstraintsStorage = std::set<std::string_view>;

ExistenceConstraintsStorage CreateExistenceConstraintsForLabel(
    const std::string_view label, const mgp::Value &properties_val, mgp_graph *memgraph_graph,
    const auto &record_factory, const std::set<std::string_view> &existing_existence_constraints) {
  ExistenceConstraintsStorage asserted_existence_constraints;
  if (!properties_val.IsList()) {
    return asserted_existence_constraints;
  }

  const auto &properties = properties_val.ValueList();
  std::for_each(properties.begin(), properties.end(),
                [&label, &existing_existence_constraints, &asserted_existence_constraints, &memgraph_graph,
                 &record_factory](const mgp::Value &property) {
                  if (!property.IsString()) {
                    return;
                  }
                  const auto property_str = property.ValueString();
                  if (property_str.empty()) {
                    return;
                  }
                  asserted_existence_constraints.emplace(property_str);
                  ProcessCreatingLabelPropertyIndexAndExistenceConstraint(
                      label, property_str, existing_existence_constraints, mgp::CreateExistenceConstraintImpl,
                      memgraph_graph, record_factory);
                });
  return asserted_existence_constraints;
}

void ProcessExistenceConstraints(const mgp::Map &existence_constraints_map, mgp_graph *memgraph_graph,
                                 const auto &record_factory, bool drop_existing) {
  auto mgp_existing_existence_constraints = mgp::ListAllExistenceConstraintsImpl(memgraph_graph);
  std::set<std::string_view> existing_existence_constraints;
  std::transform(mgp_existing_existence_constraints.begin(), mgp_existing_existence_constraints.end(),
                 std::inserter(existing_existence_constraints, existing_existence_constraints.begin()),
                 [](const mgp::Value &constraint) { return constraint.ValueString(); });

  ExistenceConstraintsStorage asserted_existence_constraints;

  auto merge_label_property = [](const std::string_view label, const std::string_view property) -> std::string {
    auto str = std::string(label) + ":";
    str += property;
    return str;
  };

  for (const auto &existing_constraint : existence_constraints_map) {
    const std::string_view label = existing_constraint.key;
    const mgp::Value &properties_val = existing_constraint.value;
    auto asserted_existence_constraints_new = CreateExistenceConstraintsForLabel(
        label, properties_val, memgraph_graph, record_factory, existing_existence_constraints);
    if (!drop_existing) {
      continue;
    }

    std::for_each(asserted_existence_constraints_new.begin(), asserted_existence_constraints_new.end(),
                  [&asserted_existence_constraints, &merge_label_property, label](const std::string_view property) {
                    asserted_existence_constraints.emplace(merge_label_property(label, property));
                  });
  }

  if (!drop_existing) {
    return;
  }

  std::set<std::string_view> existence_constraints_to_drop;
  std::set_difference(existing_existence_constraints.begin(), existing_existence_constraints.end(),
                      asserted_existence_constraints.begin(), asserted_existence_constraints.end(),
                      std::inserter(existence_constraints_to_drop, existence_constraints_to_drop.begin()));

  auto decouple_label_property = [](const std::string_view label_property) -> std::pair<std::string, std::string> {
    const auto label_size = label_property.find(':');
    const auto label = std::string(label_property.substr(0, label_size));
    const auto property = std::string(label_property.substr(label_size + 1));
    return std::make_pair(label, property);
  };

  std::for_each(existence_constraints_to_drop.begin(), existence_constraints_to_drop.end(),
                [&](const std::string_view label_property) {
                  const auto [label, property] = decouple_label_property(label_property);
                  if (mgp::DropExistenceConstraintImpl(memgraph_graph, label, property)) {
                    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, "Dropped");
                  }
                });
}

using AssertedUniqueConstraintsStorage = std::vector<std::vector<std::string_view>>;
AssertedUniqueConstraintsStorage CreateUniqueConstraintsForLabel(
    const std::string_view label, const mgp::Value &unique_props_nested,
    const std::map<std::string_view, std::vector<std::vector<std::string_view>>> &existing_unique_constraints,
    mgp_graph *memgraph_graph, const auto &record_factory) {
  AssertedUniqueConstraintsStorage asserted_unique_constraints;
  if (!unique_props_nested.IsList()) {
    return asserted_unique_constraints;
  }

  const auto unique_props_nested_list = unique_props_nested.ValueList();
  for (const auto &properties : unique_props_nested_list) {
    if (!properties.IsList()) {
      continue;
    }
    auto properties_list = properties.ValueList();
    bool unique_constraint_should_be_created = true;
    std::vector<std::string_view> properties_vec;
    properties_vec.reserve(properties_list.Size());
    for (const auto &property : properties_list) {
      if (!property.IsString() || property.ValueString().empty()) {
        unique_constraint_should_be_created = false;
        break;
      }
      properties_vec.emplace_back(property.ValueString());
    }
    if (!unique_constraint_should_be_created) {
      continue;
    }

    bool constraint_exists = false;

    const auto &existing_unique_constraints_for_label = existing_unique_constraints.find(label);
    if (existing_unique_constraints_for_label != existing_unique_constraints.end()) {
      const auto &existing_unique_constraints_for_label_vec = existing_unique_constraints_for_label->second;
      for (const auto &existing_unique_constraint : existing_unique_constraints_for_label_vec) {
        if (std::equal(existing_unique_constraint.begin(), existing_unique_constraint.end(), properties_vec.begin(),
                       properties_vec.end())) {
          constraint_exists = true;
          break;
        }
      }
    }
    asserted_unique_constraints.emplace_back(std::move(properties_vec));

    if (constraint_exists) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, "Kept");
    } else if (mgp::CreateUniqueConstraintImpl(memgraph_graph, label, properties.ptr())) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, "Created");
    }
  }
  return asserted_unique_constraints;
}

void ProcessUniqueConstraints(const mgp::Map &unique_constraints_map, mgp_graph *memgraph_graph,
                              const auto &record_factory, bool drop_existing) {
  auto mgp_existing_unique_constraints = mgp::ListAllUniqueConstraintsImpl(memgraph_graph);
  // label-unique_constraints pair
  std::map<std::string_view, std::vector<std::vector<std::string_view>>> existing_unique_constraints;
  for (const auto &constraint : mgp_existing_unique_constraints) {
    auto constraint_list = constraint.ValueList();
    std::vector<std::string_view> properties;
    properties.reserve(constraint_list.Size() - 1);
    for (int i = 1; i < constraint_list.Size(); i++) {
      properties.emplace_back(constraint_list[i].ValueString());
    }
    const std::string_view label = constraint_list[0].ValueString();
    auto [it, inserted] = existing_unique_constraints.emplace(label, std::vector<std::vector<std::string_view>>());
    it->second.emplace_back(std::move(properties));
  }

  std::map<std::string_view, std::vector<std::vector<std::string_view>>> asserted_unique_constraints;

  for (const auto &[label, unique_props_nested] : unique_constraints_map) {
    auto asserted_unique_constraints_new = CreateUniqueConstraintsForLabel(
        label, unique_props_nested, existing_unique_constraints, memgraph_graph, record_factory);
    if (!drop_existing) {
      continue;
    }
    asserted_unique_constraints.emplace(label, std::move(asserted_unique_constraints_new));
  }

  if (!drop_existing) {
    return;
  }

  std::vector<std::pair<std::string_view, std::vector<std::string_view>>> unique_constraints_to_drop;
  std::for_each(
      existing_unique_constraints.begin(), existing_unique_constraints.end(),
      [&asserted_unique_constraints, &unique_constraints_to_drop](const auto &existing_label_unique_constraints) {
        const auto &label = existing_label_unique_constraints.first;
        const auto &existing_unique_constraints_for_label = existing_label_unique_constraints.second;
        const auto &asserted_unique_constraints_for_label = asserted_unique_constraints.find(label);
        if (asserted_unique_constraints_for_label == asserted_unique_constraints.end()) {
          /// No entry for label, so we drop all unique constraints for label
          std::for_each(existing_unique_constraints_for_label.begin(), existing_unique_constraints_for_label.end(),
                        [&unique_constraints_to_drop, &label](const std::vector<std::string_view> &unique_constraint) {
                          unique_constraints_to_drop.emplace_back(label, unique_constraint);
                        });
        } else {
          const auto &asserted_unique_constraints_for_label_vec = asserted_unique_constraints_for_label->second;
          std::for_each(existing_unique_constraints_for_label.begin(), existing_unique_constraints_for_label.end(),
                        [&unique_constraints_to_drop, &label,
                         &asserted_unique_constraints_for_label_vec](const auto &existing_unique_constraint_for_label) {
                          const auto &it = std::find(asserted_unique_constraints_for_label_vec.begin(),
                                                     asserted_unique_constraints_for_label_vec.end(),
                                                     existing_unique_constraint_for_label);
                          if (it == asserted_unique_constraints_for_label_vec.end()) {
                            unique_constraints_to_drop.emplace_back(label, existing_unique_constraint_for_label);
                          }
                        });
        }
      });

  std::for_each(unique_constraints_to_drop.begin(), unique_constraints_to_drop.end(),
                [memgraph_graph, record_factory](const auto &label_unique_constraint) {
                  const auto &[label, unique_constraint] = label_unique_constraint;
                  auto unique_constraint_list = mgp::List();
                  std::for_each(unique_constraint.begin(), unique_constraint.end(),
                                [&unique_constraint_list](const std::string_view &property) {
                                  unique_constraint_list.AppendExtend(mgp::Value(property));
                                });

                  if (mgp::DropUniqueConstraintImpl(memgraph_graph, label, mgp::Value(unique_constraint_list).ptr())) {
                    InsertRecordForUniqueConstraint(record_factory, label, unique_constraint_list, "Dropped");
                  }
                });
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
  ProcessUniqueConstraints(unique_constraints_map, memgraph_graph, record_factory, drop_existing);
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
