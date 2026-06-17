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

#include "storage/v2/versioning/version_registry.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <unordered_set>

namespace memgraph::storage {

namespace {
uint64_t ParseU64(std::string_view text, uint64_t fallback) {
  uint64_t value = fallback;
  std::from_chars(text.data(), text.data() + text.size(), value);
  return value;
}

// Per-name record encoding: [8 bytes number LE][4 bytes description length LE][description][parent].
std::string EncodeInfo(const VersionInfo &info) {
  std::string out;
  for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((info.number >> (8 * i)) & 0xFF));
  const auto desc_len = static_cast<uint32_t>(info.description.size());
  for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((desc_len >> (8 * i)) & 0xFF));
  out.append(info.description);
  out.append(info.parent);
  return out;
}

VersionInfo DecodeInfo(std::string_view value) {
  VersionInfo info;
  if (value.size() < 12) return info;  // malformed; treat as empty record
  for (int i = 0; i < 8; ++i) info.number |= static_cast<uint64_t>(static_cast<uint8_t>(value[i])) << (8 * i);
  uint32_t desc_len = 0;
  for (int i = 0; i < 4; ++i) desc_len |= static_cast<uint32_t>(static_cast<uint8_t>(value[8 + i])) << (8 * i);
  if (12 + desc_len > value.size()) return info;  // malformed
  info.description = std::string{value.substr(12, desc_len)};
  info.parent = std::string{value.substr(12 + desc_len)};
  return info;
}
}  // namespace

std::recursive_mutex &VersioningStoreMutex() {
  static std::recursive_mutex mutex;
  return mutex;
}

VersionRegistry::VersionRegistry(const std::filesystem::path &versions_dir)
    : store_lock_(VersioningStoreMutex()), store_(std::make_unique<kvstore::KVStore>(versions_dir / ".registry")) {}

uint64_t VersionRegistry::Add(const std::string &name, const std::string &description, const std::string &parent) {
  uint64_t next = kMasterVersionNumber + 1;  // branches start right after master
  if (auto counter = store_->Get(kCounterKey)) {
    next = ParseU64(*counter, next);
  }
  if (!store_->Put(name, EncodeInfo({.number = next, .description = description, .parent = parent})) ||
      !store_->Put(kCounterKey, std::to_string(next + 1))) {
    throw kvstore::KVStoreError("Failed to assign a version number to '{}'.", name);
  }
  return next;
}

std::optional<VersionInfo> VersionRegistry::InfoOf(const std::string &name) const {
  auto value = store_->Get(name);
  if (!value) return std::nullopt;
  return DecodeInfo(*value);
}

void VersionRegistry::Remove(const std::string &name) { store_->Delete(name); }

std::vector<std::pair<std::string, VersionInfo>> VersionRegistry::List() const {
  std::vector<std::pair<std::string, VersionInfo>> versions;
  for (auto it = store_->begin(); it != store_->end(); ++it) {
    if (!it->first.empty() && it->first.front() == '.') continue;  // skip reserved keys (e.g. the counter)
    versions.emplace_back(it->first, DecodeInfo(it->second));
  }
  return versions;
}

bool VersionRegistry::HasChildren(const std::string &name) const {
  for (auto it = store_->begin(); it != store_->end(); ++it) {
    if (!it->first.empty() && it->first.front() == '.') continue;  // skip reserved keys
    if (DecodeInfo(it->second).parent == name) return true;
  }
  return false;
}

std::vector<std::string> VersionRegistry::AncestorChain(const std::string &name) const {
  std::vector<std::string> chain;
  std::unordered_set<std::string> seen;  // guard against accidental cycles
  std::string current = name;
  while (!current.empty() && current != "master" && seen.insert(current).second) {
    chain.push_back(current);
    auto info = InfoOf(current);
    if (!info) break;
    current = info->parent;
  }
  std::ranges::reverse(chain);  // top-level branch first, leaf last
  return chain;
}

}  // namespace memgraph::storage
