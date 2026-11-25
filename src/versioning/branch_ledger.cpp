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

#include "versioning/branch_ledger.hpp"

#include <set>
#include <stdexcept>
#include <string_view>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include "versioning/branch_info.hpp"

namespace memgraph::versioning {

namespace {

// What Read accepts and Write persists.
std::optional<std::string> Validate(const LedgerContents &contents) {
  if (contents.next_branch_id < 2) return fmt::format("next_branch_id {} is below 2", contents.next_branch_id);
  std::set<std::string_view> names;
  std::set<uint64_t> ids;
  for (const auto &[name, info] : contents.branches) {
    if (const auto valid = ValidateBranchName(name); !valid) return fmt::format("branch '{}': {}", name, valid.error());
    if (info.id < 2 || info.id >= contents.next_branch_id) {
      return fmt::format(
          "branch '{}' has id {} outside [2, next_branch_id {})", name, info.id, contents.next_branch_id);
    }
    if (!names.insert(name).second) return fmt::format("branch '{}' is listed twice", name);
    if (!ids.insert(info.id).second) return fmt::format("branch id {} is used twice", info.id);
  }
  return std::nullopt;
}

// get<uint64_t>() would silently convert a negative or fractional number.
uint64_t Unsigned(const nlohmann::json &value, const char *key) {
  if (!value.is_number_unsigned()) throw std::invalid_argument(fmt::format("'{}' is not an unsigned integer", key));
  return value.get<uint64_t>();
}

std::string Serialize(const LedgerContents &contents) {
  nlohmann::json branches = nlohmann::json::array();
  for (const auto &[name, info] : contents.branches) {
    branches.push_back(nlohmann::json{{"name", name},
                                      {"id", info.id},
                                      {"durable_fork_ts", info.fork.durable_fork_ts},
                                      {"main_durable_ts", info.fork.main_durable_ts},
                                      {"vertex_gid_floor", info.fork.vertex_gid_floor},
                                      {"edge_gid_floor", info.fork.edge_gid_floor},
                                      {"fork_vertex_count", info.fork.counts.vertices},
                                      {"fork_edge_count", info.fork.counts.edges},
                                      {"created_at_us", info.created_at_us}});
  }
  const nlohmann::json root{{"version", BranchLedger::kVersion},
                            {"next_branch_id", contents.next_branch_id},
                            {"branches", std::move(branches)}};
  return root.dump();
}

std::expected<LedgerContents, std::string> Deserialize(std::string_view payload) {
  LedgerContents out;
  try {
    const auto root = nlohmann::json::parse(payload);
    const auto version = Unsigned(root.at("version"), "version");
    if (version != BranchLedger::kVersion) throw std::invalid_argument(fmt::format("unsupported version {}", version));
    out.next_branch_id = Unsigned(root.at("next_branch_id"), "next_branch_id");
    const auto &branches = root.at("branches");
    if (!branches.is_array()) throw std::invalid_argument("'branches' is not an array");
    for (const auto &b : branches) {
      out.branches.push_back(
          {.name = b.at("name").get<std::string>(),
           .info = {.id = Unsigned(b.at("id"), "id"),
                    .fork = {.durable_fork_ts = Unsigned(b.at("durable_fork_ts"), "durable_fork_ts"),
                             .main_durable_ts = Unsigned(b.at("main_durable_ts"), "main_durable_ts"),
                             .vertex_gid_floor = Unsigned(b.at("vertex_gid_floor"), "vertex_gid_floor"),
                             .edge_gid_floor = Unsigned(b.at("edge_gid_floor"), "edge_gid_floor"),
                             .counts = {.vertices = Unsigned(b.at("fork_vertex_count"), "fork_vertex_count"),
                                        .edges = Unsigned(b.at("fork_edge_count"), "fork_edge_count")}},
                    .created_at_us = Unsigned(b.at("created_at_us"), "created_at_us")}});
    }
  } catch (const nlohmann::json::exception &e) {
    return std::unexpected(fmt::format("malformed payload: {}", e.what()));
  } catch (const std::invalid_argument &e) {
    return std::unexpected(fmt::format("malformed payload: {}", e.what()));
  }
  if (auto invalid = Validate(out)) return std::unexpected(std::move(*invalid));
  return out;
}

}  // namespace

BranchLedger::BranchLedger(kvstore::KVStore &store, const utils::UUID &db_uuid)
    : store_(store), key_(std::string{kBranchLedgerKeyPrefix} + std::string{db_uuid}) {}

std::expected<void, std::string> BranchLedger::Write(const LedgerContents &contents) const {
  // Also keeps dump() from throwing: valid names are ASCII.
  if (auto invalid = Validate(contents)) {
    return std::unexpected(fmt::format("Branch ledger: refusing to write an invalid registry: {}.", *invalid));
  }
  if (!store_.Put(key_, Serialize(contents)) || !store_.SyncWal()) {
    return std::unexpected(fmt::format("Branch ledger: cannot persist '{}'.", key_));
  }
  return {};
}

std::expected<std::optional<LedgerContents>, std::string> BranchLedger::Read() const {
  const auto value = store_.Get(key_);
  if (!value) return std::optional<LedgerContents>{};
  auto parsed = Deserialize(*value);
  if (!parsed) return std::unexpected(fmt::format("Branch ledger '{}' is unreadable: {}.", key_, parsed.error()));
  return std::optional<LedgerContents>{std::move(*parsed)};
}

}  // namespace memgraph::versioning
