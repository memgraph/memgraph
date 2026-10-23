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

#include "storage/v2/config.hpp"

#include <nlohmann/json.hpp>

namespace memgraph::storage {

void to_json(nlohmann::json &data, SalientConfig::Items const &items) {
  data = nlohmann::json::object();
  SalientConfigItemsForEachField(items, [&](const char *key, const auto &value) { data[key] = value; });
}

void from_json(const nlohmann::json &data, SalientConfig::Items &items) {
  // Single tolerant lookup per field: a key absent from a durability file written before that field
  // existed leaves the struct default in place instead of throwing (storage_light_edge precedent).
  SalientConfigItemsForEachField(items, [&](const char *key, auto &value) {
    if (auto it = data.find(key); it != data.end()) {
      it->get_to(value);
    }
  });
}

void to_json(nlohmann::json &data, SalientConfig const &config) {
  data = nlohmann::json::object();
  SalientConfigForEachField(config, [&](const char *key, const auto &value) { data[key] = value; });
}

void from_json(const nlohmann::json &data, SalientConfig &config) {
  SalientConfigForEachField(config, [&](const char *key, auto &value) {
    if (auto it = data.find(key); it != data.end()) {
      it->get_to(value);
    }
  });
}
}  // namespace memgraph::storage
