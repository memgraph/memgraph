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
#include "system/state.hpp"
#include "system/transaction.hpp"

namespace memgraph::parameters {

enum class ParameterScope : uint8_t { GLOBAL };

std::string_view ParameterScopeToString(ParameterScope scope);

struct ParameterInfo {
  std::string name;
  std::string value;
  ParameterScope scope;
};

/**
 * Parameters are dynamic key-value pairs that can be set, retrieved, and deleted.
 */
struct Parameters {
  /**
   * @brief Construct Parameters with storage path.
   * @param storage_path Path to the storage directory for parameters
   */
  explicit Parameters(const std::filesystem::path &storage_path);

  /**
   * @brief Set a parameter with a given name, value, and scope.
   * @param txn If non-null, the change is recorded in the transaction for replication/recovery.
   */
  bool SetParameter(std::string_view name, std::string_view value, ParameterScope scope = ParameterScope::GLOBAL,
                    system::Transaction *txn = nullptr);

  /**
   * @brief Get a parameter with a given name and scope.
   */
  std::optional<std::string> GetParameter(std::string_view name, ParameterScope scope = ParameterScope::GLOBAL) const;

  /**
   * @brief Delete a parameter with a given name and scope.
   * @param txn If non-null, the change is recorded in the transaction for replication/recovery.
   */
  bool UnsetParameter(std::string_view name, ParameterScope scope = ParameterScope::GLOBAL,
                      system::Transaction *txn = nullptr);

  /**
   * @brief Get all parameters with a given scope.
   */
  std::vector<ParameterInfo> GetAllParameters(ParameterScope scope) const;

  /**
   * @brief Return the number of parameters for a given scope.
   */
  size_t CountParameters(ParameterScope scope = ParameterScope::GLOBAL) const;

  /**
   * @brief Delete all parameters.
   * @param txn If non-null, the change is recorded in the transaction for replication/recovery.
   */
  bool DeleteAllParameters(system::Transaction *txn = nullptr);

  /**
   * @brief Apply parameter recovery snapshot from main (used by SystemRecoveryHandler).
   * Applied atomically: either all parameters are written or none.
   * @return true on success, false on storage error.
   */
  bool ApplyRecovery(const std::vector<ParameterInfo> &params);

  /**
   * @brief Return snapshot of all parameters for SystemRecoveryReq (main side).
   */
  std::vector<ParameterInfo> GetSnapshotForRecovery() const;

 private:
  kvstore::KVStore storage_;
};

}  // namespace memgraph::parameters
