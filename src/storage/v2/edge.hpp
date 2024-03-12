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

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "utils/logging.hpp"
#include "utils/rw_spin_lock.hpp"

#include "storage/v2/property_disk_store.hpp"

// #include "storage/v2/property_disk_store.hpp"

namespace memgraph::storage {

struct Vertex;

struct Edge {
  Edge(Gid gid, Delta *delta) : gid(gid), deleted(false), delta(delta) {
    MG_ASSERT(delta == nullptr || delta->action == Delta::Action::DELETE_OBJECT ||
                  delta->action == Delta::Action::DELETE_DESERIALIZED_OBJECT,
              "Edge must be created with an initial DELETE_OBJECT delta!");
  }

  ~Edge() {
    // TODO: Don't want to do this here
    if (!moved) ClearProperties();
  }

  Edge(Edge &) = delete;
  Edge &operator=(Edge &) = delete;
  Edge(Edge &&) = default;
  Edge &operator=(Edge &&) = delete;

  Gid gid;

  // PropertyStore properties;

  mutable utils::RWSpinLock lock;
  bool deleted;
  // uint8_t PAD;
  // uint16_t PAD;
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

  Gid HotFixForGID() const { return Gid::FromUint(gid.AsUint() + (1 << 31)); }

  PropertyValue GetProperty(PropertyId property) const {
    if (deleted) return {};
    const auto prop = PDS::get()->Get(HotFixForGID(), property);
    if (prop) return *prop;
    return {};
  }

  bool SetProperty(PropertyId property, const PropertyValue &value) {
    if (deleted) return {};
    return PDS::get()->Set(HotFixForGID(), property, value);
  }

  template <typename TContainer>
  bool InitProperties(const TContainer &properties) {
    if (deleted) return {};
    auto *pds = PDS::get();
    for (const auto &[property, value] : properties) {
      if (value.IsNull()) {
        continue;
      }
      if (!pds->Set(HotFixForGID(), property, value)) {
        return false;
      }
    }
    return true;
  }

  void ClearProperties() {
    auto *pds = PDS::get();
    pds->Clear(HotFixForGID());
  }

  std::map<PropertyId, PropertyValue> Properties() {
    if (deleted) return {};
    return PDS::get()->Get(HotFixForGID());
  }

  std::vector<std::tuple<PropertyId, PropertyValue, PropertyValue>> UpdateProperties(
      std::map<PropertyId, PropertyValue> &properties) {
    if (deleted) return {};
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
    if (deleted) return {};
    return PDS::get()->GetSize(HotFixForGID(), property);
  }
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const Edge &first, const Edge &second) { return first.gid == second.gid; }
inline bool operator<(const Edge &first, const Edge &second) { return first.gid < second.gid; }
inline bool operator==(const Edge &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const Edge &first, const Gid &second) { return first.gid < second; }

struct EdgeMetadata {
  EdgeMetadata(Gid gid, Vertex *from_vertex) : gid(gid), from_vertex(from_vertex) {}

  Gid gid;
  Vertex *from_vertex;
};

static_assert(alignof(Edge) >= 8, "The Edge should be aligned to at least 8!");

inline bool operator==(const EdgeMetadata &first, const EdgeMetadata &second) { return first.gid == second.gid; }
inline bool operator<(const EdgeMetadata &first, const EdgeMetadata &second) { return first.gid < second.gid; }
inline bool operator==(const EdgeMetadata &first, const Gid &second) { return first.gid == second; }
inline bool operator<(const EdgeMetadata &first, const Gid &second) { return first.gid < second; }

}  // namespace memgraph::storage
