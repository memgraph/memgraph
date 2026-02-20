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

#include "parameters/parameters.hpp"
#include "parameters/parameters_rpc.hpp"
#include "replication/include/replication/replication_client.hpp"
#include "replication/include/replication/state.hpp"
#include "system/include/system/action.hpp"
#include "system/include/system/transaction.hpp"

namespace memgraph::parameters {

namespace {

std::string_view ScopePrefix(ParameterScope scope) {
  switch (scope) {
    case ParameterScope::GLOBAL:
      return "global/";
    default:
      std::unreachable();
  }
}

std::string MakeKey(ParameterScope scope, std::string_view name) {
  return fmt::format("{}{}", ScopePrefix(scope), name);
}

}  // namespace

// --- System actions for replication (defined before Parameters methods that use them) ---

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
        ParameterInfo{.name = name_, .value = value_, .scope = scope_});
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
        check, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), name_, scope_);
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

std::string_view ParameterScopeToString(ParameterScope scope) {
  switch (scope) {
    case ParameterScope::GLOBAL:
      return "global";
    default:
      std::unreachable();
  }
}

Parameters::Parameters(const std::filesystem::path &storage_path) : storage_(storage_path) {}

bool Parameters::SetParameter(std::string_view name, std::string_view value, ParameterScope scope,
                              system::Transaction *txn) {
  if (!storage_.Put(MakeKey(scope, name), value)) return false;
  if (txn) txn->AddAction<SetParameterAction>(name, value, scope);
  return true;
}

std::optional<std::string> Parameters::GetParameter(std::string_view name, ParameterScope scope) const {
  return storage_.Get(MakeKey(scope, name));
}

bool Parameters::UnsetParameter(std::string_view name, ParameterScope scope, system::Transaction *txn) {
  if (!storage_.Delete(MakeKey(scope, name))) return false;
  if (txn) txn->AddAction<UnsetParameterAction>(name, scope);
  return true;
}

std::vector<ParameterInfo> Parameters::GetAllParameters(ParameterScope scope) const {
  std::vector<ParameterInfo> parameters;
  const std::string prefix(ScopePrefix(scope));
  parameters.reserve(storage_.Size(prefix));
  for (auto it = storage_.begin(prefix); it != storage_.end(prefix); ++it) {
    std::string name = it->first.substr(prefix.size());
    parameters.emplace_back(ParameterInfo{.name = std::move(name), .value = it->second, .scope = scope});
  }
  return parameters;
}

size_t Parameters::CountParameters() const { return storage_.Size(); }

bool Parameters::DeleteAllParameters(system::Transaction *txn) {
  std::vector<std::string> keys_to_delete;
  for (auto scope : {ParameterScope::GLOBAL}) {
    const std::string prefix(ScopePrefix(scope));
    for (auto it = storage_.begin(prefix); it != storage_.end(prefix); ++it) {
      keys_to_delete.push_back(it->first);
    }
  }
  if (!storage_.DeleteMultiple(keys_to_delete)) return false;
  if (txn) txn->AddAction<DeleteAllParametersAction>();
  return true;
}

bool Parameters::ApplyRecovery(const std::vector<ParameterInfo> &params) {
  std::map<std::string, std::string> items;
  for (const auto &p : params) {
    items[MakeKey(p.scope, p.name)] = p.value;
  }
  return storage_.PutMultiple(items);
}

std::vector<ParameterInfo> Parameters::GetSnapshotForRecovery() const {
  std::vector<ParameterInfo> out;
  for (auto scope : {ParameterScope::GLOBAL}) {
    auto params = GetAllParameters(scope);
    out.insert(out.end(), std::make_move_iterator(params.begin()), std::make_move_iterator(params.end()));
  }
  return out;
}

}  // namespace memgraph::parameters
