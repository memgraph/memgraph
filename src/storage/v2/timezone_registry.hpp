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

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>

#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/temporal.hpp"

namespace memgraph::storage {

/// Gives a timezone named in the time zone database a small id, so a record can name one
/// without holding either its name, which is as long as it is, or a pointer into the
/// database, which means nothing outside the process that read it.
///
/// One registry for the process, because a timezone means the same thing in every database,
/// unlike a property id. That is what keeps it out of everything that reads a record.
///
/// The ids are this registry's own rather than the database's ordering, so reloading the
/// database cannot change what an id already issued means: a reloaded zone is a zone this
/// registry has not seen, and gets an id of its own.
class TimezoneRegistry {
 public:
  TimezoneRegistry(TimezoneRegistry const &) = delete;
  TimezoneRegistry &operator=(TimezoneRegistry const &) = delete;
  TimezoneRegistry(TimezoneRegistry &&) = delete;
  TimezoneRegistry &operator=(TimezoneRegistry &&) = delete;

  static auto Instance() -> TimezoneRegistry &;

  /// The id standing for `zone`, which has to be one named in the database. A timezone given
  /// as an offset is a number in a known range and is written as itself.
  auto Intern(utils::Timezone const &zone) -> uint16_t;

  auto Resolve(uint16_t id) const -> utils::Timezone;

  /// How many distinct timezones have been named. Bounded by the database rather than by the
  /// data, since an alias resolves to the zone it stands for.
  auto size() const -> std::size_t;

 private:
  TimezoneRegistry() = default;
  ~TimezoneRegistry() = default;

  /// The database holds a few hundred zones and a record can only name one of those, so one
  /// run of slots is enough and no id is ever reused.
  static constexpr uint16_t kMaxZones = 4096;

  struct State {
    /// Keyed on the database's own object for the zone, which is one per zone, so an alias
    /// and the name it stands for intern to one id.
    std::map<std::chrono::time_zone const *, uint16_t> by_zone;
  };

  mutable utils::Synchronized<State, utils::RWSpinLock> state_;
  std::array<std::atomic<std::chrono::time_zone const *>, kMaxZones> zones_{};
  std::atomic<uint16_t> count_{0};
};

}  // namespace memgraph::storage
