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

#include "storage_service.hpp"

#include <algorithm>
#include <functional>
#include <iterator>
#include <stdexcept>

#include "interface/gen-cpp2/storage_types.h"
#include "spdlog/spdlog.h"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/result.hpp"

namespace {
// The response in the handler functions are const, so we cannot move out from them
memgraph::storage::PropertyValue ThriftValueToPropertyValue(const interface::storage::Value &value) {
  using Type = interface::storage::Value::Type;
  using PropertyValue = memgraph::storage::PropertyValue;
  using ThriftValue = interface::storage::Value;
  switch (value.getType()) {
    case Type::null_v:
      return PropertyValue{};
    case Type::bool_v:
      return PropertyValue{value.get_bool_v()};
    case Type::int_v:
      return PropertyValue{value.get_int_v()};
    case Type::double_v:
      return PropertyValue{value.get_double_v()};
    case Type::string_v:
      return PropertyValue{value.get_string_v()};
    case Type::list_v: {
      std::vector<PropertyValue> result;
      const auto &list = value.get_list_v();
      result.resize(list.size());
      std::transform(list.begin(), list.end(), std::back_insert_iterator(result), ThriftValueToPropertyValue);
      return PropertyValue{std::move(result)};
    }
    case Type::map_v: {
      std::map<std::string, PropertyValue> result;
      const auto &map = value.get_map_v();
      std::transform(map->begin(), map->end(), std::inserter(result, result.end()),
                     [](const std::pair<std::string, ThriftValue> &p) -> decltype(result)::value_type {
                       return {std::move(p.first), ThriftValueToPropertyValue(p.second)};
                     });
      return PropertyValue{std::move(result)};
    }
    case Type::date_v:
    case Type::local_time_v:
    case Type::local_date_time_v:
    case Type::duration_v:
    case Type::__EMPTY__:
    case Type::vertex_v:
    case Type::edge_v:
    case Type::path_v:
      // TODO(antaljanosbenjamin): Handle temporal types, assert on entities
      return PropertyValue{};
  }
}

interface::storage::Value PropertyValueToThriftValue(memgraph::storage::PropertyValue &&value) {
  using Type = memgraph::storage::PropertyValue::Type;
  using PropertyValue = memgraph::storage::PropertyValue;
  using ThriftValue = interface::storage::Value;
  interface::storage::Value thrift_value {};

  switch (value.type()) {
    case Type::Null:
      thrift_value.set_null_v();
      break;
    case Type::Bool:
      thrift_value.set_bool_v(value.ValueBool());
      break;
    case Type::Int:
      thrift_value.set_int_v(value.ValueInt());
      break;
    case Type::Double:
      thrift_value.set_double_v(value.ValueDouble());
      break;
    case Type::String:
      thrift_value.set_string_v(std::move(value.ValueString()));
      break;
    case Type::List: {
      std::vector<ThriftValue> result;
      auto list = std::move(value.ValueList());
      result.resize(list.size());
      std::transform(std::make_move_iterator(list.begin()), std::make_move_iterator(list.end()),
                     std::back_insert_iterator(result), PropertyValueToThriftValue);
      thrift_value.set_list_v(std::move(result));
      break;
    }
    case Type::Map: {
      std::unordered_map<std::string, ThriftValue> result{};
      auto map = std::move(value.ValueMap());
      std::transform(std::make_move_iterator(map.begin()), std::make_move_iterator(map.end()),
                     std::inserter(result, result.end()),
                     [](std::pair<std::string, PropertyValue> &&p) -> decltype(result)::value_type {
                       return {std::move(p.first), PropertyValueToThriftValue(std::move(p.second))};
                     });
      thrift_value.set_map_v(std::move(result));
    }
    case Type::TemporalData:
      // TODO(antaljanosbenjamin): Handle temporal types, assert on entities
      break;
  }
  return thrift_value;
}
static const interface::storage::Value kNullValue = std::invoke([] {
  interface::storage::Value value;
  value.set_null_v();
  return value;
});

}  // namespace

namespace manual::storage {
int64_t StorageServiceHandler::startTransaction() {
  spdlog::info("Starting transaction");
  static std::atomic<int64_t> counter{0};
  const auto transaction_id = ++counter;
  active_transactions_.insert(transaction_id, std::make_shared<memgraph::storage::Storage::Accessor>(db_.Access()));
  return transaction_id;
};

void StorageServiceHandler::commitTransaction(::interface::storage::Result &result, int64_t transaction_id) {
  spdlog::info("Commiting transaction");
  if (auto accessor_it = active_transactions_.find(transaction_id); accessor_it != active_transactions_.end()) {
    result.success_ref() = accessor_it->second->Commit().HasError();
  } else {
    result.success_ref() = false;
  }
};

void StorageServiceHandler::abortTransaction(int64_t transaction_id) {
  spdlog::info("Aborting transaction");
  if (auto accessor_it = active_transactions_.find(transaction_id); accessor_it != active_transactions_.end()) {
    accessor_it->second->Abort();
  }
};

void StorageServiceHandler::createVertices(::interface::storage::Result &result,
                                           const ::interface::storage::CreateVerticesRequest &req) {
  spdlog::info("Creating vertex...");
  result.success_ref() = false;
  auto accessor = active_transactions_.at(req.get_transaction_id());
  const auto &labels_map = req.get_labels_name_map();
  const auto &property_names_map = req.get_property_name_map();
  // auto accessor = db_.Access();

  for (auto &new_vertex : *req.new_vertices_ref()) {
    auto vertex = accessor->CreateVertex();
    for (const auto label_id : new_vertex.get_label_ids()) {
      if (const auto result = vertex.AddLabel(accessor->NameToLabel(labels_map.at(label_id))); result.HasError()) {
        return;
      }
    }

    for (auto &[prop_id, prop] : *new_vertex.properties_ref()) {
      if (const auto result = vertex.SetProperty(accessor->NameToProperty(property_names_map.at(prop_id)),
                                                 ThriftValueToPropertyValue(prop));
          result.HasError()) {
        return;
      }
    }
  }

  spdlog::info("Vertex creation done!");
}

static_assert(sizeof(apache::thrift::optional_field_ref<interface::storage::Values &>) > 16);

std::function<memgraph::utils::BasicResult<std::string>(memgraph::storage::VertexAccessor)> CreateVertexProcessor(
    memgraph::storage::Storage::Accessor &db_accessor, interface::storage::Values &values,
    apache::thrift::optional_field_ref<std::unordered_map<int64_t, std::string> &> property_name_map_ref,
    const apache::thrift::optional_field_ref<const std::vector<::std::string> &> &props_to_return_ref,
    const memgraph::storage::View view) {
  if (!props_to_return_ref.has_value()) {
    values.set_mapped();

    auto &mapped_values = values.mutable_mapped();
    auto result_props_ref = mapped_values.properties_ref();
    property_name_map_ref.ensure();
    auto &property_name_map = *property_name_map_ref;

    static constexpr auto vertex_id_id = 1L;
    auto internal_id_to_thrift_id = [&db_accessor, &property_name_map,
                                     cache = std::unordered_map<memgraph::storage::PropertyId, int64_t>{},
                                     next_id = vertex_id_id + 1l](memgraph::storage::PropertyId prop_id) mutable {
      if (const auto it = cache.find(prop_id); it != cache.end()) {
        return it->second;
      }

      const auto &name = db_accessor.PropertyToName(prop_id);
      property_name_map.emplace(next_id, name);
      cache.emplace(prop_id, next_id);
      return next_id++;
    };
    return [result_props_ref = std::move(result_props_ref), view,
            internal_id_to_thrift_id = std::move(internal_id_to_thrift_id)](
               memgraph::storage::VertexAccessor vertex_acc) mutable -> memgraph::utils::BasicResult<std::string> {
      auto props_res = vertex_acc.Properties(view);
      if (props_res.HasError()) {
        // TODO(antaljanosbenjamin): More fine grained error handling
        return std::string{"Vertex deleted"};
      }
      interface::storage::ValuesMap values_map;
      auto row_ref = values_map.values_map_ref();
      interface::storage::Value vertex_id;
      vertex_id.set_int_v(vertex_acc.Gid().AsInt());
      row_ref->reserve(props_res->size() + 1);
      row_ref->emplace(vertex_id_id, std::move(vertex_id));
      for (auto &[prop_id, prop] : *props_res) {
        const auto thrift_id = internal_id_to_thrift_id(prop_id);
        row_ref->emplace(thrift_id, PropertyValueToThriftValue(std::move(prop)));
      }
      result_props_ref->push_back(std::move(values_map));
      return {};
    };
  }
  values.set_listed({});
  auto &listed_values = values.mutable_listed();
  auto result_props_ref = listed_values.properties_ref();
  if (props_to_return_ref->empty()) {
    // Return only the vertex ids
    return [result_props_ref = std::move(result_props_ref),
            view](memgraph::storage::VertexAccessor vertex_acc) mutable -> memgraph::utils::BasicResult<std::string> {
      if (!vertex_acc.IsVisible(view)) {
        return {};
      }
      auto &values_list = result_props_ref->emplace_back();
      auto &value = values_list.emplace_back();
      value.set_int_v(vertex_acc.Gid().AsInt());
      return {};
    };
  }

  // Return properties in order
  std::vector<memgraph::storage::PropertyId> prop_ids_to_return{};
  prop_ids_to_return.reserve(props_to_return_ref->size());
  std::transform(props_to_return_ref->begin(), props_to_return_ref->end(), std::back_inserter(prop_ids_to_return),
                 std::bind_front(&memgraph::storage::Storage::Accessor::NameToProperty, &db_accessor));

  return [result_props_ref = std::move(result_props_ref), prop_ids_to_return = std::move(prop_ids_to_return),
          view](memgraph::storage::VertexAccessor vertex_acc) mutable -> memgraph::utils::BasicResult<std::string> {
    if (!vertex_acc.IsVisible(view)) {
      return {};
    }
    std::vector<interface::storage::Value> row{};
    row.reserve(prop_ids_to_return.size() + 1);
    {
      interface::storage::Value vertex_id;
      vertex_id.set_int_v(vertex_acc.Gid().AsInt());
      row.push_back(std::move(vertex_id));
    }
    for (const auto &prop_id : prop_ids_to_return) {
      auto property_result = vertex_acc.GetProperty(prop_id, view);
      if (property_result.HasError()) {
        // TODO(antaljanosbenjamin): More fine grained error handling
        return std::string{"Vertex deleted"};
      }
      row.push_back(PropertyValueToThriftValue(std::move(property_result.GetValue())));
    }
    result_props_ref->push_back(std::move(row));
    return {};
  };
}

void StorageServiceHandler::createEdges(::interface::storage::Result &result,
                                        const ::interface::storage::CreateEdgesRequest &req) {
  spdlog::info("Creating edges...");
  result.success_ref() = false;
  auto accessor = active_transactions_.at(req.get_transaction_id());
  const auto &property_names_map = req.get_property_name_map();

  for (auto &new_edge : *req.new_edges_ref()) {
    const auto src = new_edge.src().value();
    const auto dest = new_edge.dest().value();
    const auto type = new_edge.type()->name().value();

    auto from_node = accessor->FindVertex(memgraph::storage::Gid::FromInt(src), memgraph::storage::View::NEW);
    if (!from_node) throw std::runtime_error("Source node must be in the storage");
    auto to_node = accessor->FindVertex(memgraph::storage::Gid::FromInt(dest), memgraph::storage::View::NEW);
    if (!to_node) throw std::runtime_error("Destination node must be in the storage");

    auto relationship = accessor->CreateEdge(&*from_node, &*to_node, accessor->NameToEdgeType(type));
  }
  spdlog::info("Edges creation done!");
}

void StorageServiceHandler::scanVertices(::interface::storage::ScanVerticesResponse &resp,
                                         const ::interface::storage::ScanVerticesRequest &req) {
  resp.result_ref()->success_ref() = false;
  auto accessor = active_transactions_.at(req.get_transaction_id());
  // TODO(antaljanosbenjamin): handle filter
  const auto view = req.get_view();
  auto vertices = accessor->Vertices(view);
  auto it = vertices.begin();
  if (const auto *start_id = req.get_start_id(); start_id != nullptr) {
    it = vertices.IterateFrom(memgraph::storage::Gid::FromInt(*start_id));
  }
  auto count = 0;
  auto vertex_processor = CreateVertexProcessor(*accessor, *resp.values(), resp.property_name_map_ref(),
                                                req.props_to_return_ref(), req.get_view());

  const auto limit = std::invoke([&]() -> int64_t {
    if (req.limit_ref().has_value()) {
      return *req.limit_ref();
    }
    return 50;
  });

  while (count < limit && it != vertices.end()) {
    if (const auto res = vertex_processor(*it); res.HasError()) {
      throw std::runtime_error{res.GetError()};
    };
    ++it;
    ++count;
  }
  if (it != vertices.end()) {
    resp.next_start_id_ref() = (*it).Gid().AsInt();
  }
  resp.result_ref()->success_ref() = true;
}
}  // namespace manual::storage
