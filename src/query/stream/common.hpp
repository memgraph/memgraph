// Copyright 2021 Memgraph Ltd.
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

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include <json/json.hpp>

namespace query {
template <typename TMessage>
using ConsumerFunction = std::function<void(const std::vector<TMessage> &)>;

struct CommonStreamInfo {
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
  std::string transformation_name;
};

const std::string kCommonInfoKey = "common_info";

void to_json(nlohmann::json &data, CommonStreamInfo &&info);
void from_json(const nlohmann::json &data, CommonStreamInfo &common_info);
}  // namespace query
