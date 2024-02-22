// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <boost/functional/hash.hpp>
#include <mgp.hpp>
#include "utils/string.hpp"

#include <unordered_set>

namespace Schema {

constexpr std::string_view kStatusKept = "Kept";
constexpr std::string_view kStatusCreated = "Created";
constexpr std::string_view kStatusDropped = "Dropped";
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

struct PropertyInfo {
  std::unordered_set<std::string> property_types;  // property types
  int64_t number_of_property_occurrences = 0;

  PropertyInfo() = default;
  explicit PropertyInfo(std::string property_type)
      : property_types({property_type}), number_of_property_occurrences(1) {}
};

struct LabelsInfo {
  std::unordered_map<std::string, PropertyInfo> properties;  // key is a property name
  int64_t number_of_label_occurrences = 0;
};

struct LabelsHash {
  std::size_t operator()(const std::set<std::string> &s) const { return boost::hash_range(s.begin(), s.end()); }
};

struct LabelsComparator {
  bool operator()(const std::set<std::string> &lhs, const std::set<std::string> &rhs) const { return lhs == rhs; }
};

void Schema::NodeTypeProperties(mgp_list * /*args*/, mgp_graph *memgraph_graph, mgp_result *result,
                                mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  try {
    std::unordered_map<std::set<std::string>, LabelsInfo, LabelsHash, LabelsComparator> node_types_properties;

    for (auto node : mgp::Graph(memgraph_graph).Nodes()) {
      std::set<std::string> labels_set = {};
      for (auto label : node.Labels()) {
        labels_set.emplace(label);
      }

      node_types_properties[labels_set].number_of_label_occurrences++;

      if (node.Properties().empty()) {
        continue;
      }

      auto &labels_info = node_types_properties.at(labels_set);
      for (auto const &[key, prop] : node.Properties()) {
        auto const &prop_type = TypeOf(prop.Type());
        if (labels_info.properties.find(key) == labels_info.properties.end()) {
          labels_info.properties[key] = PropertyInfo{prop_type};
        } else {
          labels_info.properties[key].property_types.emplace(prop_type);
          labels_info.properties[key].number_of_property_occurrences++;
        }
      }
    }

    for (auto &[labels, labels_info] : node_types_properties) {
      std::string label_type;
      mgp::List labels_list = mgp::List();
      for (auto const &label : labels) {
        label_type += ":`" + std::string(label) + "`";
        labels_list.AppendExtend(mgp::Value(label));
      }
      for (auto const &prop : labels_info.properties) {
        auto prop_types = mgp::List();
        for (auto const &prop_type : prop.second.property_types) {
          prop_types.AppendExtend(mgp::Value(prop_type));
        }
        bool mandatory = prop.second.number_of_property_occurrences == labels_info.number_of_label_occurrences;
        auto record = record_factory.NewRecord();
        ProcessPropertiesNode(record, label_type, labels_list, prop.first, prop_types, mandatory);
      }
      if (labels_info.properties.empty()) {
        auto record = record_factory.NewRecord();
        ProcessPropertiesNode<mgp::List>(record, label_type, labels_list, "", mgp::List(), false);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Schema::RelTypeProperties(mgp_list * /*args*/, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};

  std::unordered_map<std::string, LabelsInfo> rel_types_properties;
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph = mgp::Graph(memgraph_graph);
    for (auto rel : graph.Relationships()) {
      std::string rel_type = std::string(rel.Type());
      if (rel_types_properties.find(rel_type) == rel_types_properties.end()) {
        rel_types_properties[rel_type] = LabelsInfo();
      }

      rel_types_properties[rel_type].number_of_label_occurrences++;

      if (rel.Properties().empty()) {
        continue;
      }

      auto &property_info = rel_types_properties.at(rel_type);
      for (auto &[key, prop] : rel.Properties()) {
        const auto prop_type = TypeOf(prop.Type());
        if (property_info.properties.find(key) == property_info.properties.end()) {
          property_info.properties[key] = PropertyInfo{prop_type};
        } else {
          property_info.properties[key].property_types.emplace(prop_type);
          property_info.properties[key].number_of_property_occurrences++;
        }
      }
    }

    for (auto &[type, property_info] : rel_types_properties) {
      std::string type_str = ":`" + std::string(type) + "`";
      for (auto const &prop : property_info.properties) {
        auto prop_types = mgp::List();
        for (auto const &prop_type : prop.second.property_types) {
          prop_types.AppendExtend(mgp::Value(prop_type));
        }
        bool mandatory = prop.second.number_of_property_occurrences == property_info.number_of_label_occurrences;
        auto record = record_factory.NewRecord();
        ProcessPropertiesRel(record, type_str, prop.first, prop_types, mandatory);
      }
      if (property_info.properties.empty()) {
        auto record = record_factory.NewRecord();
        ProcessPropertiesRel<mgp::List>(record, type_str, "", mgp::List(), false);
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void InsertRecordForLabelIndex(const auto &record_factory, const std::string_view label,
                               const std::string_view status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), "");
  record.Insert(std::string(Schema::kReturnKeys).c_str(), mgp::List());
  record.Insert(std::string(Schema::kReturnUnique).c_str(), false);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void InsertRecordForUniqueConstraint(const auto &record_factory, const std::string_view label,
                                     const mgp::List &properties, const std::string_view status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), properties.ToString());
  record.Insert(std::string(Schema::kReturnKeys).c_str(), properties);
  record.Insert(std::string(Schema::kReturnUnique).c_str(), true);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void InsertRecordForLabelPropertyIndexAndExistenceConstraint(const auto &record_factory, const std::string_view label,
                                                             const std::string_view property,
                                                             const std::string_view status) {
  auto record = record_factory.NewRecord();
  record.Insert(std::string(Schema::kReturnLabel).c_str(), label);
  record.Insert(std::string(Schema::kReturnKey).c_str(), property);
  record.Insert(std::string(Schema::kReturnKeys).c_str(), mgp::List({mgp::Value(property)}));
  record.Insert(std::string(Schema::kReturnUnique).c_str(), false);
  record.Insert(std::string(Schema::kReturnAction).c_str(), status);
}

void ProcessCreatingLabelIndex(const std::string_view label, const std::set<std::string_view> &existing_label_indices,
                               mgp_graph *memgraph_graph, const auto &record_factory) {
  if (existing_label_indices.contains(label)) {
    InsertRecordForLabelIndex(record_factory, label, Schema::kStatusKept);
  } else if (mgp::CreateLabelIndex(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, Schema::kStatusCreated);
  }
}

template <typename TFunc>
void ProcessCreatingLabelPropertyIndexAndExistenceConstraint(const std::string_view label,
                                                             const std::string_view property,
                                                             const std::set<std::string_view> &existing_collection,
                                                             const TFunc &func_creation, mgp_graph *memgraph_graph,
                                                             const auto &record_factory) {
  const auto label_property_search_key = std::string(label) + ":" + std::string(property);
  if (existing_collection.contains(label_property_search_key)) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, Schema::kStatusKept);
  } else if (func_creation(memgraph_graph, label, property)) {
    InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, Schema::kStatusCreated);
  }
}

/// We collect properties for which index was created.
using AssertedIndices = std::set<std::string, std::less<>>;
AssertedIndices CreateIndicesForLabel(const std::string_view label, const mgp::Value &properties_val,
                                      mgp_graph *memgraph_graph, const auto &record_factory,
                                      const std::set<std::string_view> &existing_label_indices,
                                      const std::set<std::string_view> &existing_label_property_indices) {
  AssertedIndices asserted_indices;
  if (!properties_val.IsList()) {
    return {};
  }
  if (const auto properties = properties_val.ValueList();
      properties.Empty() && mgp::CreateLabelIndex(memgraph_graph, label)) {
    InsertRecordForLabelIndex(record_factory, label, Schema::kStatusCreated);
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
                          label, property_str, existing_label_property_indices, mgp::CreateLabelPropertyIndex,
                          memgraph_graph, record_factory);
                      asserted_indices.emplace(property_str);
                    }
                  });
  }
  return asserted_indices;
}

void ProcessIndices(const mgp::Map &indices_map, mgp_graph *memgraph_graph, const auto &record_factory,
                    bool drop_existing) {
  auto mgp_existing_label_indices = mgp::ListAllLabelIndices(memgraph_graph);
  auto mgp_existing_label_property_indices = mgp::ListAllLabelPropertyIndices(memgraph_graph);

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

  auto merge_label_property = [](const std::string &label, const std::string &property) {
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
    std::ranges::for_each(asserted_indices_new, [&asserted_label_indices, &asserted_label_property_indices, label,
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
  std::ranges::set_difference(existing_label_indices, asserted_label_indices,
                              std::inserter(label_indices_to_drop, label_indices_to_drop.begin()));

  std::ranges::for_each(label_indices_to_drop, [memgraph_graph, &record_factory](const std::string_view label) {
    if (mgp::DropLabelIndex(memgraph_graph, label)) {
      InsertRecordForLabelIndex(record_factory, label, Schema::kStatusDropped);
    }
  });

  std::set<std::string_view> label_property_indices_to_drop;
  std::ranges::set_difference(existing_label_property_indices, asserted_label_property_indices,
                              std::inserter(label_property_indices_to_drop, label_property_indices_to_drop.begin()));

  auto decouple_label_property = [](const std::string_view label_property) {
    const auto label_size = label_property.find(':');
    const auto label = std::string(label_property.substr(0, label_size));
    const auto property = std::string(label_property.substr(label_size + 1));
    return std::make_pair(label, property);
  };

  std::ranges::for_each(label_property_indices_to_drop, [memgraph_graph, &record_factory, decouple_label_property](
                                                            const std::string_view label_property) {
    const auto [label, property] = decouple_label_property(label_property);
    if (mgp::DropLabelPropertyIndex(memgraph_graph, label, property)) {
      InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, Schema::kStatusDropped);
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

  auto validate_property = [](const mgp::Value &property) -> bool {
    return property.IsString() && !property.ValueString().empty();
  };

  const auto &properties = properties_val.ValueList();
  std::for_each(properties.begin(), properties.end(),
                [&label, &existing_existence_constraints, &asserted_existence_constraints, &memgraph_graph,
                 &record_factory, &validate_property](const mgp::Value &property) {
                  if (!validate_property(property)) {
                    return;
                  }
                  const std::string_view property_str = property.ValueString();
                  asserted_existence_constraints.emplace(property_str);
                  ProcessCreatingLabelPropertyIndexAndExistenceConstraint(
                      label, property_str, existing_existence_constraints, mgp::CreateExistenceConstraint,
                      memgraph_graph, record_factory);
                });
  return asserted_existence_constraints;
}

void ProcessExistenceConstraints(const mgp::Map &existence_constraints_map, mgp_graph *memgraph_graph,
                                 const auto &record_factory, bool drop_existing) {
  auto mgp_existing_existence_constraints = mgp::ListAllExistenceConstraints(memgraph_graph);
  std::set<std::string_view> existing_existence_constraints;
  std::transform(mgp_existing_existence_constraints.begin(), mgp_existing_existence_constraints.end(),
                 std::inserter(existing_existence_constraints, existing_existence_constraints.begin()),
                 [](const mgp::Value &constraint) { return constraint.ValueString(); });

  auto merge_label_property = [](const std::string_view label, const std::string_view property) {
    auto str = std::string(label) + ":";
    str += property;
    return str;
  };

  ExistenceConstraintsStorage asserted_existence_constraints;

  for (const auto &existing_constraint : existence_constraints_map) {
    const std::string_view label = existing_constraint.key;
    const mgp::Value &properties_val = existing_constraint.value;
    auto asserted_existence_constraints_new = CreateExistenceConstraintsForLabel(
        label, properties_val, memgraph_graph, record_factory, existing_existence_constraints);
    if (!drop_existing) {
      continue;
    }

    std::ranges::for_each(asserted_existence_constraints_new, [&asserted_existence_constraints, &merge_label_property,
                                                               label](const std::string_view property) {
      asserted_existence_constraints.emplace(merge_label_property(label, property));
    });
  }

  if (!drop_existing) {
    return;
  }

  std::set<std::string_view> existence_constraints_to_drop;
  std::ranges::set_difference(existing_existence_constraints, asserted_existence_constraints,
                              std::inserter(existence_constraints_to_drop, existence_constraints_to_drop.begin()));

  auto decouple_label_property = [](const std::string_view label_property) {
    const auto label_size = label_property.find(':');
    const auto label = std::string(label_property.substr(0, label_size));
    const auto property = std::string(label_property.substr(label_size + 1));
    return std::make_pair(label, property);
  };

  std::ranges::for_each(existence_constraints_to_drop, [&](const std::string_view label_property) {
    const auto [label, property] = decouple_label_property(label_property);
    if (mgp::DropExistenceConstraint(memgraph_graph, label, property)) {
      InsertRecordForLabelPropertyIndexAndExistenceConstraint(record_factory, label, property, Schema::kStatusDropped);
    }
  });
}

using AssertedUniqueConstraintsStorage = std::set<std::set<std::string_view>>;
AssertedUniqueConstraintsStorage CreateUniqueConstraintsForLabel(
    const std::string_view label, const mgp::Value &unique_props_nested,
    const std::map<std::string_view, AssertedUniqueConstraintsStorage> &existing_unique_constraints,
    mgp_graph *memgraph_graph, const auto &record_factory) {
  AssertedUniqueConstraintsStorage asserted_unique_constraints;
  if (!unique_props_nested.IsList()) {
    return asserted_unique_constraints;
  }

  auto validate_unique_constraint_props = [](const mgp::Value &properties) -> bool {
    if (!properties.IsList()) {
      return false;
    }
    const auto &properties_list = properties.ValueList();
    if (properties_list.Empty()) {
      return false;
    }
    return std::all_of(properties_list.begin(), properties_list.end(), [](const mgp::Value &property) {
      return property.IsString() && !property.ValueString().empty();
    });
  };

  auto unique_constraint_exists =
      [](const std::string_view label, const std::set<std::string_view> &properties,
         const std::map<std::string_view, AssertedUniqueConstraintsStorage> &existing_unique_constraints) -> bool {
    auto iter = existing_unique_constraints.find(label);
    if (iter == existing_unique_constraints.end()) {
      return false;
    }
    return iter->second.find(properties) != iter->second.end();
  };

  for (const auto unique_props_nested_list = unique_props_nested.ValueList();
       const auto &properties : unique_props_nested_list) {
    if (!validate_unique_constraint_props(properties)) {
      continue;
    }
    const auto properties_list = properties.ValueList();
    std::set<std::string_view> properties_coll;
    std::transform(properties_list.begin(), properties_list.end(),
                   std::inserter(properties_coll, properties_coll.begin()),
                   [](const mgp::Value &property) { return property.ValueString(); });

    if (unique_constraint_exists(label, properties_coll, existing_unique_constraints)) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, Schema::kStatusKept);
    } else if (mgp::CreateUniqueConstraint(memgraph_graph, label, properties.ptr())) {
      InsertRecordForUniqueConstraint(record_factory, label, properties_list, Schema::kStatusCreated);
    }
    asserted_unique_constraints.emplace(std::move(properties_coll));
  }
  return asserted_unique_constraints;
}

void ProcessUniqueConstraints(const mgp::Map &unique_constraints_map, mgp_graph *memgraph_graph,
                              const auto &record_factory, bool drop_existing) {
  auto mgp_existing_unique_constraints = mgp::ListAllUniqueConstraints(memgraph_graph);
  // label-unique_constraints pair
  std::map<std::string_view, AssertedUniqueConstraintsStorage> existing_unique_constraints;
  for (const auto &constraint : mgp_existing_unique_constraints) {
    auto constraint_list = constraint.ValueList();
    std::set<std::string_view> properties;
    for (int i = 1; i < constraint_list.Size(); i++) {
      properties.emplace(constraint_list[i].ValueString());
    }
    const std::string_view label = constraint_list[0].ValueString();
    auto [it, inserted] = existing_unique_constraints.try_emplace(label, AssertedUniqueConstraintsStorage{properties});
    if (!inserted) {
      it->second.emplace(std::move(properties));
    }
  }

  std::map<std::string_view, AssertedUniqueConstraintsStorage> asserted_unique_constraints;

  for (const auto &[label, unique_props_nested] : unique_constraints_map) {
    auto asserted_unique_constraints_new = CreateUniqueConstraintsForLabel(
        label, unique_props_nested, existing_unique_constraints, memgraph_graph, record_factory);
    if (drop_existing) {
      asserted_unique_constraints.emplace(label, std::move(asserted_unique_constraints_new));
    }
  }

  if (!drop_existing) {
    return;
  }

  std::vector<std::pair<std::string_view, std::set<std::string_view>>> unique_constraints_to_drop;

  // Check for each label for we found existing constraint in the DB whether it was asserted.
  // If no unique constraint was found with label, we can drop all unique constraints for this label. (if branch)
  // If some unique constraint was found with label, we can drop only those unique constraints that were not asserted.
  // (else branch.)
  std::ranges::for_each(existing_unique_constraints, [&asserted_unique_constraints, &unique_constraints_to_drop](
                                                         const auto &existing_label_unique_constraints) {
    const auto &label = existing_label_unique_constraints.first;
    const auto &existing_unique_constraints_for_label = existing_label_unique_constraints.second;
    const auto &asserted_unique_constraints_for_label = asserted_unique_constraints.find(label);
    if (asserted_unique_constraints_for_label == asserted_unique_constraints.end()) {
      std::ranges::for_each(
          std::make_move_iterator(existing_unique_constraints_for_label.begin()),
          std::make_move_iterator(existing_unique_constraints_for_label.end()),
          [&unique_constraints_to_drop, &label](std::set<std::string_view> existing_unique_constraint_for_label) {
            unique_constraints_to_drop.emplace_back(label, std::move(existing_unique_constraint_for_label));
          });
    } else {
      const auto &asserted_unique_constraints_for_label_coll = asserted_unique_constraints_for_label->second;
      std::ranges::for_each(
          std::make_move_iterator(existing_unique_constraints_for_label.begin()),
          std::make_move_iterator(existing_unique_constraints_for_label.end()),
          [&unique_constraints_to_drop, &label, &asserted_unique_constraints_for_label_coll](
              std::set<std::string_view> existing_unique_constraint_for_label) {
            if (!asserted_unique_constraints_for_label_coll.contains(existing_unique_constraint_for_label)) {
              unique_constraints_to_drop.emplace_back(label, std::move(existing_unique_constraint_for_label));
            }
          });
    }
  });
  std::ranges::for_each(
      unique_constraints_to_drop, [memgraph_graph, &record_factory](const auto &label_unique_constraint) {
        const auto &[label, unique_constraint] = label_unique_constraint;

        auto unique_constraint_list = mgp::List();
        std::ranges::for_each(unique_constraint, [&unique_constraint_list](const std::string_view &property) {
          unique_constraint_list.AppendExtend(mgp::Value(property));
        });

        if (mgp::DropUniqueConstraint(memgraph_graph, label, mgp::Value(unique_constraint_list).ptr())) {
          InsertRecordForUniqueConstraint(record_factory, label, unique_constraint_list, Schema::kStatusDropped);
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
