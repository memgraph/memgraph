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

// What Read accepts and Write persists. A parent need only be a valid name, not a listed branch.
std::optional<std::string> Validate(const LedgerContents &contents) {
  if (contents.next_number < 2) return fmt::format("next_number {} is below 2", contents.next_number);
  std::set<std::string_view> names;
  std::set<uint64_t> numbers;
  for (const auto &[name, info] : contents.branches) {
    if (const auto valid = ValidateBranchName(name); !valid) return fmt::format("branch '{}': {}", name, valid.error());
    if (info.parent != "main") {
      if (const auto valid = ValidateBranchName(info.parent); !valid) {
        return fmt::format("branch '{}' has parent '{}': {}", name, info.parent, valid.error());
      }
    }
    if (info.parent == name) return fmt::format("branch '{}' is its own parent", name);
    if (info.number < 2 || info.number >= contents.next_number) {
      return fmt::format(
          "branch '{}' has number {} outside [2, next_number {})", name, info.number, contents.next_number);
    }
    if (info.vertex_gid_floor && info.vertex_gid_ceiling && *info.vertex_gid_ceiling < *info.vertex_gid_floor) {
      return fmt::format("branch '{}' has a vertex gid ceiling below its floor", name);
    }
    if (info.edge_gid_floor && info.edge_gid_ceiling && *info.edge_gid_ceiling < *info.edge_gid_floor) {
      return fmt::format("branch '{}' has an edge gid ceiling below its floor", name);
    }
    if (!names.insert(name).second) return fmt::format("branch '{}' is listed twice", name);
    if (!numbers.insert(info.number).second) return fmt::format("branch number {} is used twice", info.number);
  }
  return std::nullopt;
}

// get<uint64_t>() would silently convert a negative or fractional number.
uint64_t Unsigned(const nlohmann::json &value, const char *key) {
  if (!value.is_number_unsigned()) throw std::invalid_argument(fmt::format("'{}' is not an unsigned integer", key));
  return value.get<uint64_t>();
}

std::optional<uint64_t> OptionalUnsigned(const nlohmann::json &object, const char *key) {
  const auto it = object.find(key);
  if (it == object.end()) return std::nullopt;
  return Unsigned(*it, key);
}

std::string Serialize(const LedgerContents &contents) {
  nlohmann::json branches = nlohmann::json::array();
  for (const auto &[name, info] : contents.branches) {
    nlohmann::json b{{"name", name},
                     {"number", info.number},
                     {"parent", info.parent},
                     // Persist the recorded boundary, not the runtime pin: they differ after startup restore.
                     {"fork_ts", info.durable_fork_ts}};
    if (info.vertex_gid_floor) b["vertex_gid_floor"] = *info.vertex_gid_floor;
    if (info.edge_gid_floor) b["edge_gid_floor"] = *info.edge_gid_floor;
    if (info.vertex_gid_ceiling) b["vertex_gid_ceiling"] = *info.vertex_gid_ceiling;
    if (info.edge_gid_ceiling) b["edge_gid_ceiling"] = *info.edge_gid_ceiling;
    if (info.created_at_us) b["created_at_us"] = *info.created_at_us;
    branches.push_back(std::move(b));
  }
  const nlohmann::json root{
      {"version", BranchLedger::kVersion}, {"next_number", contents.next_number}, {"branches", std::move(branches)}};
  return root.dump();
}

std::expected<LedgerContents, std::string> Deserialize(std::string_view payload) {
  LedgerContents out;
  try {
    const auto root = nlohmann::json::parse(payload);
    const auto version = Unsigned(root.at("version"), "version");
    if (version != BranchLedger::kVersion) throw std::invalid_argument(fmt::format("unsupported version {}", version));
    out.next_number = Unsigned(root.at("next_number"), "next_number");
    const auto &branches = root.at("branches");
    if (!branches.is_array()) throw std::invalid_argument("'branches' is not an array");
    for (const auto &b : branches) {
      out.branches.push_back({.name = b.at("name").get<std::string>(),
                              .info = {.number = Unsigned(b.at("number"), "number"),
                                       .parent = b.at("parent").get<std::string>(),
                                       .fork_ts = Unsigned(b.at("fork_ts"), "fork_ts"),
                                       .vertex_gid_floor = OptionalUnsigned(b, "vertex_gid_floor"),
                                       .edge_gid_floor = OptionalUnsigned(b, "edge_gid_floor"),
                                       .vertex_gid_ceiling = OptionalUnsigned(b, "vertex_gid_ceiling"),
                                       .edge_gid_ceiling = OptionalUnsigned(b, "edge_gid_ceiling"),
                                       .created_at_us = OptionalUnsigned(b, "created_at_us")}});
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
