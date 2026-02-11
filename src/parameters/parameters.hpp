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
#include "replication/state.hpp"
#include "system/state.hpp"
#include "system/transaction.hpp"
#include "utils/rw_lock.hpp"

namespace memgraph::parameters {

enum class ParameterScope : uint8_t { GLOBAL, DATABASE, SESSION };

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
  /**
   * @brief Construct Parameters with storage path.
   * @param storage_path Path to the storage directory for parameters
   */
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

  /**
   * @brief Delete all parameters.
   */
  bool DeleteAllParameters();

  /**
   * @brief Apply parameter recovery snapshot from main (used by SystemRecoveryHandler).
   */
  bool ApplyRecovery(const std::vector<ParameterInfo> &params);

  /**
   * @brief Return snapshot of all parameters for SystemRecoveryReq (main side).
   */
  std::vector<ParameterInfo> GetSnapshotForRecovery() const;

  /**
   * @brief Register replication RPC handlers for parameters on the replica.
   */
  void RegisterReplicationHandlers(replication::RoleReplicaData const &data,
                                   system::ReplicaHandlerAccessToState &system_state_access);

 private:
  mutable utils::RWLock parameters_lock_{utils::RWLock::Priority::WRITE};
  std::optional<kvstore::KVStore> storage_;
};

void AddSetParameterAction(system::Transaction &txn, std::string_view name, std::string_view value,
                           ParameterScope scope = ParameterScope::GLOBAL);

void AddUnsetParameterAction(system::Transaction &txn, std::string_view name,
                             ParameterScope scope = ParameterScope::GLOBAL);

void AddDeleteAllParametersAction(system::Transaction &txn);

}  // namespace memgraph::parameters
