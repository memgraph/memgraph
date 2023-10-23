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

#pragma once

#include <fmt/core.h>
#include "utils/simple_json.hpp"

namespace memgraph::storage {

struct LabelPropertyIndexStats {
  uint64_t count, distinct_values_count;
  double statistic, avg_group_size, avg_degree;
};

static inline std::string ToJson(const LabelPropertyIndexStats &in) {
  return fmt::format(
      R"({{"count":{}, "distinct_values_count":{}, "statistic":{}, "avg_group_size":{} "avg_degree":{}}})", in.count,
      in.distinct_values_count, in.statistic, in.avg_group_size, in.avg_degree);
}

static inline bool FromJson(const std::string &json, LabelPropertyIndexStats &out) {
  bool res = true;
  res &= utils::GetJsonValue(json, "count", out.count);
  res &= utils::GetJsonValue(json, "distinct_values_count", out.distinct_values_count);
  res &= utils::GetJsonValue(json, "statistic", out.statistic);
  res &= utils::GetJsonValue(json, "avg_group_size", out.avg_group_size);
  res &= utils::GetJsonValue(json, "avg_degree", out.avg_degree);
  return res;
}

}  // namespace memgraph::storage
