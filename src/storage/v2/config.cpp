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

#include "storage/v2/config.hpp"

#include <nlohmann/json.hpp>

namespace memgraph::storage {

void to_json(nlohmann::json &data, SalientConfig::Items const &items) {
  data = nlohmann::json{
      {"properties_on_edges", items.properties_on_edges},
      {"enable_schema_metadata", items.enable_schema_metadata},
      {"enable_schema_info", items.enable_schema_info},
      {"enable_edges_metadata", items.enable_edges_metadata},
      {"enable_label_index_auto_creation", items.enable_label_index_auto_creation},
      {"enable_edge_type_index_auto_creation", items.enable_edge_type_index_auto_creation},
      {"property_store_compression_enabled", items.property_store_compression_enabled},
  };
}

void from_json(const nlohmann::json &data, SalientConfig::Items &items) {
  data.at("properties_on_edges").get_to(items.properties_on_edges);
  data.at("enable_edges_metadata").get_to(items.enable_edges_metadata);
  data.at("enable_schema_metadata").get_to(items.enable_schema_metadata);
  data.at("enable_schema_info").get_to(items.enable_schema_info);
  data.at("enable_label_index_auto_creation").get_to(items.enable_label_index_auto_creation);
  data.at("enable_edge_type_index_auto_creation").get_to(items.enable_edge_type_index_auto_creation);
  data.at("property_store_compression_enabled").get_to(items.property_store_compression_enabled);
}

void to_json(nlohmann::json &data, SalientConfig const &config) {
  data = nlohmann::json{{"items", config.items},
                        {"name", config.name},
                        {"uuid", config.uuid},
                        {"storage_mode", config.storage_mode},
                        "property_store_compression_level",
                        config.property_store_compression_level};
}

void from_json(const nlohmann::json &data, SalientConfig &config) {
  data.at("items").get_to(config.items);
  data.at("name").get_to(config.name);
  data.at("uuid").get_to(config.uuid);
  data.at("storage_mode").get_to(config.storage_mode);
  data.at("property_store_compression_level").get_to(config.property_store_compression_level);
}
}  // namespace memgraph::storage
