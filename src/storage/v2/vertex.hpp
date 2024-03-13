// Copyright 2024 Memgraph Ltd.
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

#include <limits>
#include <optional>
#include <tuple>
#include <vector>

#include "storage/v2/delta.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/rw_spin_lock.hpp"

#include "storage/v2/property_disk_store.hpp"

namespace memgraph::storage {

struct Vertex {
  Vertex(Gid gid, Delta *delta) : gid(gid), deleted(false), has_prop(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Vertex must be created with an initial DELETE_OBJECT delta!");
  }

  ~Vertex() {
    // TODO: Move to another place <- this will get called twice if moved...
    if (!moved) ClearProperties();
  }

  Vertex(Vertex &) = delete;
  Vertex &operator=(Vertex &) = delete;
  Vertex(Vertex &&) noexcept = default;
  Vertex &operator=(Vertex &&) = delete;

  const Gid gid;

  std::vector<LabelId> labels;
  // PropertyStore properties;

  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
  std::vector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

  mutable utils::RWSpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;
  bool has_prop;

  class HotFixMove {
   public:
    HotFixMove() {}
    HotFixMove(HotFixMove &&other) noexcept {
      if (this != &other) {
        // We want only the latest object to be marked as not-moved; while all previous should be marked as moved
        moved = false;
        other.moved = true;
      }
    }
    HotFixMove(HotFixMove &) = delete;
    HotFixMove &operator=(HotFixMove &) = delete;
    HotFixMove &operator=(HotFixMove &&) = delete;

    operator bool() const { return moved; }

   private:
    bool moved{false};
  } moved;

  Delta *delta;

  PropertyValue GetProperty(PropertyId property) const {
    // if (deleted) return {};
    if (!has_prop) return {};
    const auto prop = PDS::get()->Get(gid, property);
    if (prop) return *prop;
    return {};
  }

  bool SetProperty(PropertyId property, const PropertyValue &value) {
    // if (deleted) return {};
    has_prop = true;
    return PDS::get()->Set(gid, property, value);
  }

  bool HasProperty(PropertyId property) const {
    // if (deleted) return {};
    if (!has_prop) return {};
    return PDS::get()->Has(gid, property);
  }

  bool HasAllProperties(const std::set<PropertyId> &properties) const {
    // if (deleted) return {};
    if (!has_prop) return {};
    return std::all_of(properties.begin(), properties.end(), [this](const auto &prop) { return HasProperty(prop); });
  }

  bool IsPropertyEqual(PropertyId property, const PropertyValue &value) const {
    // if (deleted) return {};
    if (!has_prop) return value.IsNull();
    const auto val = GetProperty(property);
    return val == value;
  }

  template <typename TContainer>
  bool InitProperties(const TContainer &properties) {
    // if (deleted) return {};
    auto *pds = PDS::get();
    for (const auto &[property, value] : properties) {
      if (value.IsNull()) {
        continue;
      }
      if (!pds->Set(gid, property, value)) {
        return false;
      }
      has_prop = true;
    }
    return true;
  }

  void ClearProperties() {
    if (!has_prop) return;
    has_prop = false;
    auto *pds = PDS::get();
    pds->Clear(gid);
  }

  std::map<PropertyId, PropertyValue> Properties() {
    // if (deleted) return {};
    if (!has_prop) return {};
    return PDS::get()->Get(gid);
  }

  std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> UpdateProperties(
      std::map<PropertyId, PropertyValue> &properties) {
    // if (deleted) return {};
    auto old_properties = Properties();
    ClearProperties();

    std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> id_old_new_change;
    id_old_new_change.reserve(properties.size() + old_properties.size());
    for (const auto &[prop_id, new_value] : properties) {
      if (!old_properties.contains(prop_id)) {
        id_old_new_change.emplace_back(prop_id, PropertyValue(), new_value);
      }
    }

    for (const auto &[old_key, old_value] : old_properties) {
      auto [it, inserted] = properties.emplace(old_key, old_value);
      if (!inserted) {
        auto &new_value = it->second;
        id_old_new_change.emplace_back(it->first, old_value, new_value);
      }
    }

    MG_ASSERT(InitProperties(properties));
    return id_old_new_change;
  }

  uint64_t PropertySize(PropertyId property) const {
    // if (deleted) return {};
    if (!has_prop) return {};
    return PDS::get()->GetSize(gid, property);
  }

  std::optional<std::vector<PropertyValue>> ExtractPropertyValues(const std::set<PropertyId> &properties) const {
    // if (deleted) return {};
    if (!has_prop) return {};
    std::vector<PropertyValue> value_array;
    value_array.reserve(properties.size());
    for (const auto &prop : properties) {
      auto value = GetProperty(prop);
      if (value.IsNull()) {
        return std::nullopt;
      }
      value_array.emplace_back(std::move(value));
    }
    return value_array;
  }
};

static_assert(alignof(Vertex) >= 8, "The Vertex should be aligned to at least 8!");

inline bool operator==(const Vertex &first, const Vertex &second) { return first.gid == second.gid; }
inline bool operator<(const Vertex &first, const Vertex &second) { return first.gid < second.gid; }
inline bool operator==(const Vertex &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Vertex &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
