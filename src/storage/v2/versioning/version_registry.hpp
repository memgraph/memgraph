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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "kvstore/kvstore.hpp"

namespace memgraph::storage {

// The base graph is always version number 1.
inline constexpr uint64_t kMasterVersionNumber = 1;

// Metadata recorded for a named version.
struct VersionInfo {
  uint64_t number{0};
  std::string description;  // WITH DESCRIPTION '<str>' (empty string when omitted)
  std::string parent;       // BRANCH FROM '<parent>' ("master" for a top-level branch)
};

// Per-database registry mapping a version name to its auto-assigned, monotonically increasing number
// and optional description. `master` is implicitly number 1; created branches get 2, 3, ... The
// counter never rolls back, so a number is never reused even after a version is dropped. Backed by
// its own RocksDB instance at <versions_dir>/.registry (reserved name — versions can't start with '.').
class VersionRegistry {
 public:
  explicit VersionRegistry(const std::filesystem::path &versions_dir);

  VersionRegistry(VersionRegistry &&) noexcept = default;
  VersionRegistry &operator=(VersionRegistry &&) noexcept = default;
  VersionRegistry(const VersionRegistry &) = delete;
  VersionRegistry &operator=(const VersionRegistry &) = delete;
  ~VersionRegistry() = default;

  // Assigns and persists the next monotonic number for `name` (description may be empty; parent is
  // the parent version name, "master" for a top-level branch), returning the assigned number.
  uint64_t Add(const std::string &name, const std::string &description, const std::string &parent);

  // Metadata previously recorded for `name`, if any.
  std::optional<VersionInfo> InfoOf(const std::string &name) const;

  // Forgets `name` (the counter is intentionally not decremented).
  void Remove(const std::string &name);

  // (name, info) for every branch (excludes master), unordered.
  std::vector<std::pair<std::string, VersionInfo>> List() const;

  // Ancestor chain for `name`, ordered from the top-level branch (whose parent is master) down to
  // `name` itself. Excludes master. Used to replay overlays in order onto the base graph.
  std::vector<std::string> AncestorChain(const std::string &name) const;

 private:
  static constexpr std::string_view kCounterKey = ".counter";
  std::unique_ptr<kvstore::KVStore> store_;
};

}  // namespace memgraph::storage
