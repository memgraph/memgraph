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

#pragma once

#include <cstdint>
#include <string>

namespace memgraph::storage {

struct LabelPropertyIndexStats {
  uint64_t count, distinct_values_count;
  double statistic, avg_group_size, avg_degree;

  auto operator<=>(const LabelPropertyIndexStats &) const = default;
};

std::string ToJson(const LabelPropertyIndexStats &in);

bool FromJson(const std::string &json, LabelPropertyIndexStats &out);

}  // namespace memgraph::storage
