// Copyright 2022 Memgraph Ltd.
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
#include <map>
#include <set>
#include <vector>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "common/types.hpp"
#include "coordinator/hybrid_logical_clock.hpp"
#include "io/address.hpp"
#include "storage/v3/config.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/temporal.hpp"

namespace memgraph::coordinator {

constexpr int64_t kNotExistingId{0};

using memgraph::io::Address;
using memgraph::storage::v3::Config;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyId;
using memgraph::storage::v3::PropertyValue;
using memgraph::storage::v3::SchemaProperty;

enum class Status : uint8_t {
  CONSENSUS_PARTICIPANT,
  INITIALIZING,
  // TODO(tyler) this will possibly have more states,
  // depending on the reconfiguration protocol that we
  // implement.
};

struct AddressAndStatus {
  memgraph::io::Address address;
  Status status;
  friend bool operator<(const AddressAndStatus &lhs, const AddressAndStatus &rhs) { return lhs.address < rhs.address; }
};

using PrimaryKey = std::vector<PropertyValue>;
using Shard = std::vector<AddressAndStatus>;
using Shards = std::map<PrimaryKey, Shard>;
using LabelName = std::string;
using PropertyName = std::string;
using PropertyMap = std::map<PropertyName, PropertyId>;

struct ShardToInitialize {
  boost::uuids::uuid uuid;
  LabelId label_id;
  PrimaryKey min_key;
  std::optional<PrimaryKey> max_key;
  std::vector<SchemaProperty> schema;
  Config config;
};

PrimaryKey SchemaToMinKey(const std::vector<SchemaProperty> &schema);

struct LabelSpace {
  std::vector<SchemaProperty> schema;
  std::map<PrimaryKey, Shard> shards;
  size_t replication_factor;
};

struct ShardMap {
  Hlc shard_map_version;
  uint64_t max_property_id{kNotExistingId};
  std::map<PropertyName, PropertyId> properties;
  uint64_t max_label_id{kNotExistingId};
  std::map<LabelName, LabelId> labels;
  std::map<LabelId, LabelSpace> label_spaces;
  std::map<LabelId, std::vector<SchemaProperty>> schemas;

  Shards GetShards(const LabelName &label) {
    const auto id = labels.at(label);
    auto &shards = label_spaces.at(id).shards;
    return shards;
  }

  // TODO(gabor) later we will want to update the wallclock time with
  // the given Io<impl>'s time as well
  Hlc IncrementShardMapVersion() noexcept {
    ++shard_map_version.logical_id;
    return shard_map_version;
  }

  Hlc GetHlc() const noexcept { return shard_map_version; }

  // Returns the shard UUIDs that have been assigned but not yet acknowledged for this storage manager
  std::vector<ShardToInitialize> AssignShards(Address storage_manager, std::set<boost::uuids::uuid> initialized) {
    std::vector<ShardToInitialize> ret{};

    bool mutated = false;

    for (auto &[label_id, label_space] : label_spaces) {
      for (auto &[low_key, shard] : label_space.shards) {
        // TODO(tyler) avoid these triple-nested loops by having the heartbeat include better info
        bool machine_contains_shard = false;

        for (auto &aas : shard) {
          if (initialized.contains(aas.address.unique_id)) {
            spdlog::info("marking shard as full consensus participant: {}", aas.address.unique_id);
            aas.status = Status::CONSENSUS_PARTICIPANT;
            machine_contains_shard = true;
          } else {
            const bool same_machine = aas.address.last_known_ip == storage_manager.last_known_ip &&
                                      aas.address.last_known_port == storage_manager.last_known_port;
            if (same_machine) {
              machine_contains_shard = true;
              ret.push_back(ShardToInitialize{
                  .uuid = aas.address.unique_id,
                  .label_id = label_id,
                  .min_key = low_key,
                  .max_key = std::nullopt,
                  .schema = label_space.schema,
                  .config = Config{},
              });
            }
          }
        }

        if (!machine_contains_shard && shard.size() < label_space.replication_factor) {
          Address address = storage_manager;

          // TODO(tyler) use deterministic UUID so that coordinators don't diverge here
          address.unique_id = boost::uuids::uuid{boost::uuids::random_generator()()},

          ret.push_back(ShardToInitialize{
              .uuid = address.unique_id,
              .label_id = label_id,
              .min_key = low_key,
              .max_key = std::nullopt,
              .config = Config{},
          });

          AddressAndStatus aas = {
              .address = address,
              .status = Status::INITIALIZING,
          };

          shard.emplace_back(aas);
        }
      }
    }

    if (mutated) {
      IncrementShardMapVersion();
    }

    return ret;
  }

  bool SplitShard(Hlc previous_shard_map_version, LabelId label_id, const PrimaryKey &key) {
    if (previous_shard_map_version != shard_map_version) {
      return false;
    }

    auto &label_space = label_spaces.at(label_id);
    auto &shards_in_map = label_space.shards;

    MG_ASSERT(!shards_in_map.empty());
    MG_ASSERT(!shards_in_map.contains(key));
    MG_ASSERT(label_spaces.contains(label_id));

    // Finding the Shard that the new PrimaryKey should map to.
    auto prev = std::prev(shards_in_map.upper_bound(key));
    Shard duplicated_shard = prev->second;

    // Apply the split
    shards_in_map[key] = duplicated_shard;

    return true;
  }

  std::optional<LabelId> InitializeNewLabel(std::string label_name, std::vector<SchemaProperty> schema,
                                            size_t replication_factor, Hlc last_shard_map_version);

  void AddServer(Address server_address) {
    // Find a random place for the server to plug in
  }

  LabelId GetLabelId(const std::string &label) const { return labels.at(label); }

  Shards GetShardsForRange(const LabelName &label_name, const PrimaryKey &start_key, const PrimaryKey &end_key) const {
    MG_ASSERT(start_key <= end_key);
    MG_ASSERT(labels.contains(label_name));

    LabelId label_id = labels.at(label_name);

    const auto &label_space = label_spaces.at(label_id);

    const auto &shards_for_label = label_space.shards;

    MG_ASSERT(shards_for_label.begin()->first <= start_key,
              "the ShardMap must always contain a minimal key that is less than or equal to any requested key");

    auto it = std::prev(shards_for_label.upper_bound(start_key));
    const auto end_it = shards_for_label.upper_bound(end_key);

    Shards shards{};

    std::copy(it, end_it, std::inserter(shards, shards.end()));

    return shards;
  }

  Shard GetShardForKey(const LabelName &label_name, const PrimaryKey &key) const {
    MG_ASSERT(labels.contains(label_name));

    LabelId label_id = labels.at(label_name);

    const auto &label_space = label_spaces.at(label_id);

    MG_ASSERT(label_space.shards.begin()->first <= key,
              "the ShardMap must always contain a minimal key that is less than or equal to any requested key");

    return std::prev(label_space.shards.upper_bound(key))->second;
  }

  Shard GetShardForKey(const LabelId &label_id, const PrimaryKey &key) const {
    MG_ASSERT(label_spaces.contains(label_id));

    const auto &label_space = label_spaces.at(label_id);

    MG_ASSERT(label_space.shards.begin()->first <= key,
              "the ShardMap must always contain a minimal key that is less than or equal to any requested key");

    return std::prev(label_space.shards.upper_bound(key))->second;
  }

  PropertyMap AllocatePropertyIds(const std::vector<PropertyName> &new_properties) {
    PropertyMap ret{};

    bool mutated = false;

    for (const auto &property_name : new_properties) {
      if (properties.contains(property_name)) {
        auto property_id = properties.at(property_name);
        ret.emplace(property_name, property_id);
      } else {
        mutated = true;

        const PropertyId property_id = PropertyId::FromUint(++max_property_id);
        ret.emplace(property_name, property_id);
        properties.emplace(property_name, property_id);
      }
    }

    if (mutated) {
      IncrementShardMapVersion();
    }

    return ret;
  }

  std::optional<PropertyId> GetPropertyId(const std::string &property_name) const {
    if (properties.contains(property_name)) {
      return properties.at(property_name);
    }

    return std::nullopt;
  }
};

}  // namespace memgraph::coordinator
