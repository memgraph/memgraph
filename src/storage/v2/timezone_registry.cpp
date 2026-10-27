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

#include "storage/v2/timezone_registry.hpp"

#include <optional>
#include <variant>

#include "utils/logging.hpp"

namespace memgraph::storage {

auto TimezoneRegistry::Instance() -> TimezoneRegistry & {
  static auto registry = TimezoneRegistry{};
  return registry;
}

auto TimezoneRegistry::Intern(utils::Timezone const &zone) -> uint16_t {
  MG_ASSERT(zone.InTzDatabase(), "Only a timezone named in the database is given an id");
  auto const *named = std::get<std::chrono::time_zone const *>(zone.GetOffset());

  // Nearly every call is for a zone already seen, so look under a read lock and take the
  // write lock only to name a new one.
  auto const existing = state_.WithReadLock([&](State const &state) -> std::optional<uint16_t> {
    auto const it = state.by_zone.find(named);
    if (it == state.by_zone.end()) return std::nullopt;
    return it->second;
  });
  if (existing) return *existing;

  return state_.WithLock([&](State &state) {
    // Another thread may have named it between the two locks.
    auto const it = state.by_zone.find(named);
    if (it != state.by_zone.end()) return it->second;

    auto const id = count_.load(std::memory_order_relaxed);
    MG_ASSERT(id < kMaxZones, "A process names at most {} timezones", kMaxZones);

    // The slot is filled before the count that admits it, so a reader that can see an id
    // sees the zone it stands for.
    zones_[id].store(named, std::memory_order_release);
    count_.store(static_cast<uint16_t>(id + 1), std::memory_order_release);
    state.by_zone.emplace(named, id);
    return id;
  });
}

auto TimezoneRegistry::Resolve(uint16_t id) const -> utils::Timezone {
  // No lock: the run of slots never moves and a filled one is never cleared.
  DMG_ASSERT(id < count_.load(std::memory_order_acquire), "Resolving a timezone id this registry never issued");
  auto const *named = zones_[id].load(std::memory_order_acquire);
  return utils::Timezone{named};
}

auto TimezoneRegistry::size() const -> std::size_t { return count_.load(std::memory_order_acquire); }

}  // namespace memgraph::storage
