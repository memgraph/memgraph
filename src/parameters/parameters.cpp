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

#include <fmt/format.h>

#include "flags/general.hpp"
#include "parameters/parameters.hpp"
#include "parameters/parameters_rpc.hpp"
#include "replication/include/replication/replication_client.hpp"
#include "replication/include/replication/state.hpp"
#include "system/include/system/action.hpp"
#include "system/include/system/transaction.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"

namespace memgraph::parameters {

namespace {

std::string_view ScopePrefix(ParameterScope scope) {
  switch (scope) {
    case ParameterScope::GLOBAL:
      return "global/";
    case ParameterScope::DATABASE:
      return "database/";
    case ParameterScope::SESSION:
      return "session/";
  }
  throw utils::BasicException("Invalid parameter scope");
}

std::string MakeKey(ParameterScope scope, std::string_view name) {
  return std::string(ScopePrefix(scope)) + std::string(name);
}

}  // namespace

std::string_view ParameterScopeToString(ParameterScope scope) {
  switch (scope) {
    case ParameterScope::GLOBAL:
      return "global";
    case ParameterScope::DATABASE:
      return "database";
    case ParameterScope::SESSION:
      return "session";
  }
  throw utils::BasicException("Invalid parameter scope");
}

Parameters::Parameters(const std::filesystem::path &storage_path) {
  if (!FLAGS_data_recovery_on_startup && utils::DirExists(storage_path)) {
    utils::DeleteDir(storage_path);
  }
  storage_.emplace(storage_path);
}

bool Parameters::SetParameter(std::string_view name, std::string_view value, ParameterScope scope) {
  MG_ASSERT(storage_);
  return storage_->Put(MakeKey(scope, name), value);
}

std::optional<std::string> Parameters::GetParameter(std::string_view name, ParameterScope scope) const {
  if (!storage_) return std::nullopt;
  return storage_->Get(MakeKey(scope, name));
}

bool Parameters::UnsetParameter(std::string_view name, ParameterScope scope) {
  MG_ASSERT(storage_);
  return storage_->Delete(MakeKey(scope, name));
}

std::vector<ParameterInfo> Parameters::GetAllParameters(ParameterScope scope) const {
  std::vector<ParameterInfo> parameters;
  if (!storage_) return parameters;
  std::string prefix(ScopePrefix(scope));
  parameters.reserve(storage_->Size(prefix));
  for (auto it = storage_->begin(prefix); it != storage_->end(prefix); ++it) {
    std::string name = it->first.substr(prefix.size());
    parameters.emplace_back(ParameterInfo{.name = std::move(name), .value = it->second, .scope = scope});
  }
  return parameters;
}

bool Parameters::DeleteAllParameters() {
  MG_ASSERT(storage_);
  std::vector<std::string> keys_to_delete;
  for (const auto scope : {ParameterScope::GLOBAL, ParameterScope::DATABASE, ParameterScope::SESSION}) {
    std::string prefix(ScopePrefix(scope));
    for (auto it = storage_->begin(prefix); it != storage_->end(prefix); ++it) {
      keys_to_delete.push_back(it->first);
    }
  }
  return storage_->DeleteMultiple(keys_to_delete);
}

bool Parameters::ApplyRecovery(const std::vector<ParameterInfo> &params) {
  MG_ASSERT(storage_);
  std::map<std::string, std::string> items;
  for (const auto &p : params) {
    items[MakeKey(p.scope, p.name)] = p.value;
  }
  return storage_->PutMultiple(items);
}

std::vector<ParameterInfo> Parameters::GetSnapshotForRecovery() const {
  std::vector<ParameterInfo> out;
  for (const auto scope : {ParameterScope::GLOBAL, ParameterScope::DATABASE, ParameterScope::SESSION}) {
    for (const auto &p : GetAllParameters(scope)) {
      out.push_back(p);
    }
  }
  return out;
}

// --- System actions for replication ---

struct SetParameterAction : memgraph::system::ISystemAction {
  explicit SetParameterAction(std::string_view name, std::string_view value, ParameterScope scope)
      : name_{name}, value_{value}, scope_{scope} {}

  void DoDurability() override { /* Done during Parameters execution */ }

  bool ShouldReplicateInCommunity() const override { return true; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check = [](const storage::replication::SetParameterRes &res) { return res.success; };
    return client.StreamAndFinalizeDelta<storage::replication::SetParameterRpc>(
        check,
        main_uuid,
        txn.last_committed_system_timestamp(),
        txn.timestamp(),
        ParameterInfo{.name = std::string(name_), .value = std::string(value_), .scope = scope_});
  }

  void PostReplication(replication::RoleMainData &) const override {}

 private:
  std::string name_;
  std::string value_;
  ParameterScope scope_;
};

struct UnsetParameterAction : memgraph::system::ISystemAction {
  explicit UnsetParameterAction(std::string_view name, ParameterScope scope) : name_{name}, scope_{scope} {}

  void DoDurability() override { /* Done during Parameters execution */ }

  bool ShouldReplicateInCommunity() const override { return true; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check = [](const storage::replication::UnsetParameterRes &res) { return res.success; };
    return client.StreamAndFinalizeDelta<storage::replication::UnsetParameterRpc>(
        check, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), std::string(name_), scope_);
  }

  void PostReplication(replication::RoleMainData &) const override {}

 private:
  std::string name_;
  ParameterScope scope_;
};

struct DeleteAllParametersAction : memgraph::system::ISystemAction {
  void DoDurability() override { /* Done during Parameters execution */ }

  bool ShouldReplicateInCommunity() const override { return true; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check = [](const storage::replication::DeleteAllParametersRes &res) { return res.success; };
    return client.StreamAndFinalizeDelta<storage::replication::DeleteAllParametersRpc>(
        check, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp());
  }

  void PostReplication(replication::RoleMainData &) const override {}
};

void AddSetParameterAction(system::Transaction &txn, std::string_view name, std::string_view value,
                           ParameterScope scope) {
  txn.AddAction<SetParameterAction>(name, value, scope);
}

void AddUnsetParameterAction(system::Transaction &txn, std::string_view name, ParameterScope scope) {
  txn.AddAction<UnsetParameterAction>(name, scope);
}

void AddDeleteAllParametersAction(system::Transaction &txn) { txn.AddAction<DeleteAllParametersAction>(); }

}  // namespace memgraph::parameters
