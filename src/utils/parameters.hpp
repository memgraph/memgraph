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

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "utils/rw_lock.hpp"

namespace memgraph::utils {

enum class ParameterScope { GLOBAL, DATABASE, SESSION };

std::string_view ParameterScopeToString(ParameterScope scope);

struct ParameterInfo {
  std::string name;
  std::string value;
  ParameterScope scope;
};

/**
 * Manages global, database, and session parameters.
 * Parameters are dynamic key-value pairs that can be set, retrieved, and deleted.
 * Unlike Settings, parameters don't need to be pre-registered and can be created on-the-fly.
 */
struct Parameters {
  explicit Parameters(std::filesystem::path storage_path);

  /**
   * @brief Set a parameter with a given name, value, and scope.
   */
  bool SetParameter(std::string_view name, std::string_view value, ParameterScope scope = ParameterScope::GLOBAL);

  /**
   * @brief Get a parameter with a given name and scope.
   */
  std::optional<std::string> GetParameter(std::string_view name, ParameterScope scope = ParameterScope::GLOBAL) const;

  /**
   * @brief Delete a parameter with a given name and scope.
   */
  bool UnsetParameter(std::string_view name, ParameterScope scope = ParameterScope::GLOBAL);

  /**
   * @brief Get all parameters with a given scope.
   */
  std::vector<ParameterInfo> GetAllParameters(ParameterScope scope) const;

 private:
  mutable utils::RWLock parameters_lock_{RWLock::Priority::WRITE};
  std::optional<kvstore::KVStore> storage_;
};

}  // namespace memgraph::utils
