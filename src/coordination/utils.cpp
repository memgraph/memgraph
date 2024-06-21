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
#include <string_view>
#include "kvstore/kvstore.hpp"
#include "nuraft/logger_wrapper.hpp"
#include "utils/logging.hpp"

namespace memgraph::coordination {
#ifdef MG_ENTERPRISE
auto GetOrSetDefaultVersion(kvstore::KVStore &durability, std::string_view key, int const default_value,
                            LoggerWrapper logger) -> int {
  auto maybe_version = durability.Get(key);
  if (maybe_version.has_value()) {
    return std::stoi(maybe_version.value());
  }
  logger.Log(
      nuraft_log_level::INFO,
      fmt::format("Assuming first start of durability as key value {} for version is missing, storing version {}.", key,
                  default_value));
  MG_ASSERT(durability.Put(key, std::to_string(default_value)), "Failed to store durability version.");
  return default_value;
}
#endif
}  // namespace memgraph::coordination
