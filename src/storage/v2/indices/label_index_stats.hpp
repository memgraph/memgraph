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

#pragma once

#include <fmt/core.h>
#include "utils/simple_json.hpp"

namespace memgraph::storage {

struct LabelIndexStats {
  uint64_t count;
  double avg_degree;
};

static inline std::string ToJson(const LabelIndexStats &in) {
  return fmt::format(R"({{"count":{}, "avg_degree":{}}})", in.count, in.avg_degree);
}

static inline bool FromJson(const std::string &json, LabelIndexStats &out) {
  bool res = true;
  res &= utils::GetJsonValue(json, "count", out.count);
  res &= utils::GetJsonValue(json, "avg_degree", out.avg_degree);
  return res;
}

}  // namespace memgraph::storage
