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

#include <algorithm>
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
#include "utils/exceptions.hpp"
#include "utils/print_helpers.hpp"

namespace memgraph::coordinator {

constexpr int64_t kNotExistingId{0};

using memgraph::io::Address;
using memgraph::storage::v3::Config;
using memgraph::storage::v3::EdgeTypeId;
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

  friend std::ostream &operator<<(std::ostream &in, const AddressAndStatus &address_and_status) {
    in << "AddressAndStatus { address: ";
    in << address_and_status.address;
    if (address_and_status.status == Status::CONSENSUS_PARTICIPANT) {
      in << ", status: CONSENSUS_PARTICIPANT }";
    } else {
      in << ", status: INITIALIZING }";
    }

    return in;
  }

  friend bool operator==(const AddressAndStatus &lhs, const AddressAndStatus &rhs) {
    return lhs.address == rhs.address;
  }
};

using PrimaryKey = std::vector<PropertyValue>;
using Shard = std::vector<AddressAndStatus>;
using Shards = std::map<PrimaryKey, Shard>;
using LabelName = std::string;
using PropertyName = std::string;
using EdgeTypeName = std::string;
using PropertyMap = std::map<PropertyName, PropertyId>;
using EdgeTypeIdMap = std::map<EdgeTypeName, EdgeTypeId>;

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
  // Maps between the smallest primary key stored in the shard and the shard
  std::map<PrimaryKey, Shard> shards;
  size_t replication_factor;

  friend std::ostream &operator<<(std::ostream &in, const LabelSpace &label_space) {
    using utils::print_helpers::operator<<;

    in << "LabelSpace { schema: ";
    in << label_space.schema;
    in << ", shards: ";
    in << label_space.shards;
    in << ", replication_factor: " << label_space.replication_factor << "}";

    return in;
  }
};

struct ShardMap {
  Hlc shard_map_version;
  uint64_t max_property_id{kNotExistingId};
  uint64_t max_edge_type_id{kNotExistingId};
  std::map<PropertyName, PropertyId> properties;
  std::map<EdgeTypeName, EdgeTypeId> edge_types;
  uint64_t max_label_id{kNotExistingId};
  std::map<LabelName, LabelId> labels;
  std::map<LabelId, LabelSpace> label_spaces;
  std::map<LabelId, std::vector<SchemaProperty>> schemas;

  [[nodiscard]] static ShardMap Parse(std::istream &input_stream);
  friend std::ostream &operator<<(std::ostream &in, const ShardMap &shard_map);

  Shards GetShards(const LabelName &label) const;

  std::vector<Shards> GetShards() const;

  // TODO(gabor) later we will want to update the wallclock time with
  // the given Io<impl>'s time as well
  Hlc IncrementShardMapVersion() noexcept;
  Hlc GetHlc() const noexcept;

  // Returns the shard UUIDs that have been assigned but not yet acknowledged for this storage manager
  std::vector<ShardToInitialize> AssignShards(Address storage_manager, std::set<boost::uuids::uuid> initialized);

  bool SplitShard(Hlc previous_shard_map_version, LabelId label_id, const PrimaryKey &key);

  std::optional<LabelId> InitializeNewLabel(std::string label_name, std::vector<SchemaProperty> schema,
                                            size_t replication_factor, Hlc last_shard_map_version);

  void AddServer(Address server_address);

  std::optional<LabelId> GetLabelId(const std::string &label) const;
  // TODO(antaljanosbenjamin): Remove this and instead use NameIdMapper
  const std::string &GetLabelName(LabelId label) const;
  std::optional<PropertyId> GetPropertyId(const std::string &property_name) const;
  const std::string &GetPropertyName(PropertyId property) const;
  std::optional<EdgeTypeId> GetEdgeTypeId(const std::string &edge_type) const;
  const std::string &GetEdgeTypeName(EdgeTypeId property) const;

  Shards GetShardsForRange(const LabelName &label_name, const PrimaryKey &start_key, const PrimaryKey &end_key) const;

  Shard GetShardForKey(const LabelName &label_name, const PrimaryKey &key) const;

  Shard GetShardForKey(const LabelId &label_id, const PrimaryKey &key) const;

  PropertyMap AllocatePropertyIds(const std::vector<PropertyName> &new_properties);

  EdgeTypeIdMap AllocateEdgeTypeIds(const std::vector<EdgeTypeName> &new_edge_types);

  /// Returns true if all shards have the desired number of replicas and they are in
  /// the CONSENSUS_PARTICIPANT state. Note that this does not necessarily mean that
  /// there is also an active leader for each shard.
  bool ClusterInitialized() const;
};

}  // namespace memgraph::coordinator
