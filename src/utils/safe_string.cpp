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

#include "utils/safe_string.hpp"

#include "slk/serialization.hpp"

#include <nlohmann/json.hpp>

namespace memgraph::utils {

void to_json(nlohmann::json &data, SafeString const &str) { data = *str.str_view(); }

void from_json(const nlohmann::json &data, SafeString &str) { str = data.get<std::string>(); }

}  // namespace memgraph::utils

// Serialize UUID
namespace memgraph::slk {
void Save(const memgraph::utils::SafeString &self, memgraph::slk::Builder *builder) {
  memgraph::slk::Save(*self.str_view(), builder);
}

void Load(memgraph::utils::SafeString *self, memgraph::slk::Reader *reader) {
  std::string str;
  memgraph::slk::Load(&str, reader);
  *self = std::move(str);
}
}  // namespace memgraph::slk
