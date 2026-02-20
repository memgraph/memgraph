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
#include "parameters/rpc.hpp"
#include "replication/include/replication/replication_client.hpp"
#include "replication/include/replication/state.hpp"
#include "system/include/system/action.hpp"
#include "system/include/system/transaction.hpp"

namespace memgraph::parameters {

namespace {

std::string KeyPrefix(std::string_view scope_context) {
  if (scope_context.empty()) return std::string(kGlobalPrefix);
  return fmt::format("{}{}/", kDatabasePrefix, scope_context);
}

std::string MakeKey(std::string_view name, std::string_view scope_context) {
  return fmt::format("{}{}", KeyPrefix(scope_context), name);
}

}  // namespace

// --- System actions for replication (defined before Parameters methods that use them) ---

struct SetParameterAction : memgraph::system::ISystemAction {
  explicit SetParameterAction(std::string_view name, std::string_view value, std::string_view scope_context = {})
      : name_{name}, value_{value}, scope_context_{scope_context} {}

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
        ParameterInfo{.name = name_, .value = value_, .scope_context = scope_context_});
  }

  void PostReplication(replication::RoleMainData &) const override {}

 private:
  std::string name_;
  std::string value_;
  std::string scope_context_;
};

struct UnsetParameterAction : memgraph::system::ISystemAction {
  explicit UnsetParameterAction(std::string_view name, std::string_view scope_context = {})
      : name_{name}, scope_context_{scope_context} {}

  void DoDurability() override { /* Done during Parameters execution */ }

  bool ShouldReplicateInCommunity() const override { return true; }

  bool DoReplication(replication::ReplicationClient &client, const utils::UUID &main_uuid,
                     memgraph::system::Transaction const &txn) const override {
    auto check = [](const storage::replication::UnsetParameterRes &res) { return res.success; };
    return client.StreamAndFinalizeDelta<storage::replication::UnsetParameterRpc>(
        check, main_uuid, txn.last_committed_system_timestamp(), txn.timestamp(), name_, scope_context_);
  }

  void PostReplication(replication::RoleMainData &) const override {}

 private:
  std::string name_;
  std::string scope_context_;
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

Parameters::Parameters(const std::filesystem::path &storage_path) : storage_(storage_path) {}

SetParameterResult Parameters::SetParameter(std::string_view name, std::string_view value,
                                            std::string_view scope_context, system::Transaction *txn) {
  if (!scope_context.empty() && GetParameter(name, {}).has_value()) return SetParameterResult::GlobalAlreadyExists;
  if (!storage_.Put(MakeKey(name, scope_context), value)) return SetParameterResult::StorageError;
  if (txn) txn->AddAction<SetParameterAction>(name, value, scope_context);
  return SetParameterResult::Success;
}

std::optional<std::string> Parameters::GetParameter(std::string_view name, std::string_view scope_context) const {
  return storage_.Get(MakeKey(name, scope_context));
}

bool Parameters::UnsetParameter(std::string_view name, std::string_view scope_context, system::Transaction *txn) {
  if (!storage_.Delete(MakeKey(name, scope_context))) return false;
  if (txn) txn->AddAction<UnsetParameterAction>(name, scope_context);
  return true;
}

std::vector<ParameterInfo> Parameters::GetGlobalParameters() const {
  std::vector<ParameterInfo> parameters;
  const std::string prefix(KeyPrefix({}));
  for (auto it = storage_.begin(prefix); it != storage_.end(prefix); ++it) {
    parameters.emplace_back(
        ParameterInfo{.name = it->first.substr(prefix.size()), .value = it->second, .scope_context = {}});
  }
  return parameters;
}

std::vector<ParameterInfo> Parameters::GetParameters(std::string_view database_uuid) const {
  auto parameters = GetGlobalParameters();
  const std::string prefix(KeyPrefix(database_uuid));
  for (auto it = storage_.begin(prefix); it != storage_.end(prefix); ++it) {
    parameters.emplace_back(ParameterInfo{
        .name = it->first.substr(prefix.size()), .value = it->second, .scope_context = std::string(database_uuid)});
  }
  return parameters;
}

size_t Parameters::CountParameters() const { return storage_.Size(); }

bool Parameters::DeleteAllParameters(system::Transaction *txn) {
  if (!storage_.DeletePrefix(std::string(kGlobalPrefix))) return false;
  if (!storage_.DeletePrefix(std::string(kDatabasePrefix))) return false;
  if (txn) txn->AddAction<DeleteAllParametersAction>();
  return true;
}

bool Parameters::ApplyRecovery(const std::vector<ParameterInfo> &params) {
  std::map<std::string, std::string> items;
  for (const auto &p : params) {
    items[MakeKey(p.name, p.scope_context)] = p.value;
  }
  return storage_.PutMultiple(items);
}

std::vector<ParameterInfo> Parameters::GetSnapshotForRecovery() const {
  auto out = GetGlobalParameters();
  const std::string prefix(kDatabasePrefix);
  for (auto it = storage_.begin(prefix); it != storage_.end(prefix); ++it) {
    const auto &key = it->first;
    auto slash = key.find('/', prefix.size());
    DMG_ASSERT(slash != std::string::npos, "Corrupted parameter key: {}", key);
    auto name = key.substr(slash + 1);
    auto scope_context = key.substr(prefix.size(), slash - prefix.size());
    out.emplace_back(
        ParameterInfo{.name = std::move(name), .value = it->second, .scope_context = std::move(scope_context)});
  }
  return out;
}

}  // namespace memgraph::parameters
