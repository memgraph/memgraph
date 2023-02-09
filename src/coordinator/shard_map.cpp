// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <boost/uuid/uuid.hpp>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/types.hpp"
#include "coordinator/hybrid_logical_clock.hpp"
#include "coordinator/shard_map.hpp"
#include "query/v2/requests.hpp"
#include "spdlog/spdlog.h"
#include "storage/v3/config.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/temporal.hpp"
#include "storage/v3/value_conversions.hpp"
#include "utils/cast.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

namespace memgraph::coordinator {

using memgraph::common::SchemaType;
using memgraph::storage::v3::TemporalData;
using memgraph::storage::v3::TemporalType;

PrimaryKey SchemaToMinKey(const std::vector<SchemaProperty> &schema) {
  PrimaryKey ret{};

  const int64_t min_int = std::numeric_limits<int64_t>::min();

  const TemporalData date{TemporalType::Date, min_int};
  const TemporalData local_time{TemporalType::LocalTime, min_int};
  const TemporalData local_date_time{TemporalType::LocalDateTime, min_int};
  const TemporalData duration{TemporalType::Duration, min_int};

  for (const auto &schema_property : schema) {
    switch (schema_property.type) {
      case SchemaType::BOOL:
        ret.emplace_back(PropertyValue(false));
        break;
      case SchemaType::INT:
        ret.emplace_back(PropertyValue(min_int));
        break;
      case SchemaType::STRING:
        ret.emplace_back(PropertyValue(""));
        break;
      case SchemaType::DATE:
        ret.emplace_back(PropertyValue(date));
        break;
      case SchemaType::LOCALTIME:
        ret.emplace_back(PropertyValue(local_time));
        break;
      case SchemaType::LOCALDATETIME:
        ret.emplace_back(PropertyValue(local_date_time));
        break;
      case SchemaType::DURATION:
        ret.emplace_back(PropertyValue(duration));
        break;
    }
  }

  return ret;
}

ShardMap ShardMap::Parse(std::istream &input_stream) {
  const uint64_t default_replication_factor = 1;
  const uint64_t default_split_threshold = 1'000'000;

  ShardMap shard_map;
  const auto read_size = [&input_stream] {
    size_t size{0};
    input_stream >> size;
    return size;
  };

  // Reads a string until the next whitespace
  const auto read_word = [&input_stream] {
    std::string word;
    input_stream >> word;
    return word;
  };

  const auto read_names = [&read_size, &read_word] {
    const auto number_of_names = read_size();
    spdlog::trace("Reading {} names", number_of_names);
    std::vector<std::string> names;
    names.reserve(number_of_names);

    for (auto name_index = 0; name_index < number_of_names; ++name_index) {
      names.push_back(read_word());
      spdlog::trace("Read '{}'", names.back());
    }
    return names;
  };

  const auto read_line = [&input_stream] {
    std::string line;
    std::getline(input_stream, line);
    return line;
  };

  const auto parse_type = [](const std::string &type) {
    static const auto type_map = std::unordered_map<std::string, common::SchemaType>{
        {"string", common::SchemaType::STRING}, {"int", common::SchemaType::INT}, {"bool", common::SchemaType::BOOL}};
    const auto lower_case_type = utils::ToLowerCase(type);
    auto it = type_map.find(lower_case_type);
    MG_ASSERT(it != type_map.end(), "Invalid type in split files: {}", type);
    return it->second;
  };

  const auto parse_property_value = [](std::string text, const common::SchemaType type) {
    if (type == common::SchemaType::STRING) {
      return storage::v3::PropertyValue{std::move(text)};
    }
    if (type == common::SchemaType::INT) {
      size_t processed{0};
      int64_t value = std::stoll(text, &processed);
      MG_ASSERT(processed == text.size() || text[processed] == ' ', "Invalid integer format: '{}'", text);
      return storage::v3::PropertyValue{value};
    }
    LOG_FATAL("Not supported type: {}", utils::UnderlyingCast(type));
  };

  spdlog::debug("Reading properties");
  const auto properties = read_names();
  MG_ASSERT(shard_map.AllocatePropertyIds(properties).size() == properties.size(),
            "Unexpected number of properties created!");

  spdlog::debug("Reading edge types");
  const auto edge_types = read_names();
  MG_ASSERT(shard_map.AllocateEdgeTypeIds(edge_types).size() == edge_types.size(),
            "Unexpected number of properties created!");

  spdlog::debug("Reading primary labels");
  const auto number_of_primary_labels = read_size();
  spdlog::debug("Reading {} primary labels", number_of_primary_labels);

  for (auto label_index = 0; label_index < number_of_primary_labels; ++label_index) {
    const auto primary_label = read_word();
    spdlog::debug("Reading primary label named '{}'", primary_label);
    const auto number_of_primary_properties = read_size();
    spdlog::debug("Reading {} primary properties", number_of_primary_properties);
    std::vector<std::string> pp_names;
    std::vector<common::SchemaType> pp_types;
    pp_names.reserve(number_of_primary_properties);
    pp_types.reserve(number_of_primary_properties);
    for (auto property_index = 0; property_index < number_of_primary_properties; ++property_index) {
      pp_names.push_back(read_word());
      spdlog::debug("Reading primary property named '{}'", pp_names.back());
      pp_types.push_back(parse_type(read_word()));
    }
    auto pp_mapping = shard_map.AllocatePropertyIds(pp_names);
    std::vector<SchemaProperty> schema;
    schema.reserve(number_of_primary_properties);

    for (auto property_index = 0; property_index < number_of_primary_properties; ++property_index) {
      schema.push_back(storage::v3::SchemaProperty{pp_mapping.at(pp_names[property_index]), pp_types[property_index]});
    }
    const auto hlc = shard_map.GetHlc();
    MG_ASSERT(
        shard_map.InitializeNewLabel(primary_label, schema, default_replication_factor, default_split_threshold, hlc)
            .has_value(),
        "Cannot initialize new label: {}", primary_label);
  }

  return shard_map;
}

std::ostream &operator<<(std::ostream &in, const ShardMap &shard_map) {
  using utils::print_helpers::operator<<;

  in << "ShardMap { shard_map_version: " << shard_map.shard_map_version;
  in << ", max_property_id: " << shard_map.max_property_id;
  in << ", max_edge_type_id: " << shard_map.max_edge_type_id;
  in << ", properties: " << shard_map.properties;
  in << ", edge_types: " << shard_map.edge_types;
  in << ", max_label_id: " << shard_map.max_label_id;
  in << ", labels: " << shard_map.labels;
  in << ", label_spaces: " << shard_map.label_spaces;
  in << ", schemas: " << shard_map.schemas;
  in << "}";
  return in;
}

Shards ShardMap::GetShardsForLabel(const LabelName &label) const {
  const auto id = labels.at(label);
  const auto &shards = label_spaces.at(id).shards;
  return shards;
}

std::vector<Shards> ShardMap::GetAllShards() const {
  std::vector<Shards> all_shards;
  all_shards.reserve(label_spaces.size());
  std::transform(label_spaces.begin(), label_spaces.end(), std::back_inserter(all_shards),
                 [](const auto &label_space) { return label_space.second.shards; });
  return all_shards;
}

// TODO(gabor) later we will want to update the wallclock time with
// the given Io<impl>'s time as well
Hlc ShardMap::IncrementShardMapVersion() noexcept {
  ++shard_map_version.logical_id;
  return shard_map_version;
}

// TODO(antaljanosbenjamin) use a single map for all name id
// mapping and a single counter to maintain the next id
std::unordered_map<uint64_t, std::string> ShardMap::IdToNames() {
  std::unordered_map<uint64_t, std::string> id_to_names;

  const auto map_type_ids = [&id_to_names](const auto &name_to_id_type) {
    for (const auto &[name, id] : name_to_id_type) {
      id_to_names.emplace(id.AsUint(), name);
    }
  };

  map_type_ids(edge_types);
  map_type_ids(labels);
  map_type_ids(properties);

  return id_to_names;
}

boost::uuids::uuid Uint64ToUuid(uint64_t u) {
  return boost::uuids::uuid{0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            static_cast<unsigned char>(u >> 56U),
                            static_cast<unsigned char>(u >> 48U),
                            static_cast<unsigned char>(u >> 40U),
                            static_cast<unsigned char>(u >> 32U),
                            static_cast<unsigned char>(u >> 24U),
                            static_cast<unsigned char>(u >> 16U),
                            static_cast<unsigned char>(u >> 8U),
                            static_cast<unsigned char>(u)};
}

boost::uuids::uuid ShardMap::NewShardUuid() {
  uint64_t shard_id = GetHlc().logical_id;
  return Uint64ToUuid(shard_id);
}

Hlc ShardMap::GetHlc() noexcept { return ++shard_map_version; }

HeartbeatResponse ShardMap::AssignShards(Address storage_manager, std::set<boost::uuids::uuid> initialized,
                                         std::set<msgs::SuggestedSplitInfo> pending_splits) {
  HeartbeatResponse ret{};

  bool mutated = false;
  std::map<std::pair<boost::uuids::uuid, Hlc>, msgs::PrimaryKey> mapped_pending_splits;
  for (const auto &pending_split : pending_splits) {
    MG_ASSERT(pending_split.shard_to_split_uuid != Uint64ToUuid(0),
              "a shard split for an impossible UUID is being requested");
    spdlog::info("Coordinator adding pending split for shard version: {}, uuid: {} to attempt to initiate",
                 pending_split.shard_version, pending_split.shard_to_split_uuid);
    mapped_pending_splits.insert(
        {{pending_split.shard_to_split_uuid, pending_split.shard_version}, pending_split.split_key});
  }

  for (auto &[label_id, label_space] : label_spaces) {
    for (auto it = label_space.shards.begin(); it != label_space.shards.end(); it++) {
      auto &[low_key, shard] = *it;
      std::optional<PrimaryKey> high_key;
      if (const auto next_it = std::next(it); next_it != label_space.shards.end()) {
        high_key = next_it->first;
      }
      // TODO(tyler) avoid these triple-nested loops by having the heartbeat include better info
      bool shard_assigned_to_machine = false;

      for (auto &peer_metadata : shard.peers) {
        const bool same_machine = peer_metadata.address.last_known_ip == storage_manager.last_known_ip &&
                                  peer_metadata.address.last_known_port == storage_manager.last_known_port;
        spdlog::info("Coordinator split debug - shard version: {}", shard.version);
        if (initialized.contains(peer_metadata.address.unique_id)) {
          spdlog::info("1");
          shard_assigned_to_machine = true;

          if (!same_machine) {
            // set the last known ip and port to the storage manager that has heartbeated it just now
            mutated = true;
            peer_metadata.address.last_known_ip = storage_manager.last_known_ip;
            peer_metadata.address.last_known_port = storage_manager.last_known_port;
          }

          if (peer_metadata.status != Status::CONSENSUS_PARTICIPANT) {
            mutated = true;
            spdlog::info("marking shard as full consensus participant: {}", peer_metadata.address.unique_id);
            peer_metadata.status = Status::CONSENSUS_PARTICIPANT;
          }
        } else if (same_machine && peer_metadata.status == Status::INITIALIZING) {
          spdlog::info("2");
          // we are expecting this shard to be initialized (rather than split) on this machine,
          // so send it an initialization request

          shard_assigned_to_machine = true;
          spdlog::info("reminding shard manager that they should begin participating in shard");

          ret.shards_to_initialize.push_back(ShardToInitialize{
              .new_shard_version = shard.version,
              .uuid = peer_metadata.address.unique_id,
              .label_id = label_id,
              .min_key = low_key,
              .max_key = high_key,
              .schema = schemas[label_id],
              .config = Config{.split =
                                   Config::Split{
                                       .max_shard_vertex_size = label_space.split_threshold,
                                   }},
              .id_to_names = IdToNames(),
          });
        } else if (same_machine && peer_metadata.status == Status::PENDING_SPLIT) {
          // we are expecting this shard to be split, so send it a split request
          spdlog::info(
              "Coordinator expecting heartbeating peer to split, so it will reply with a ShardToSplit message");

          std::map<boost::uuids::uuid, boost::uuids::uuid> uuid_mapping{};

          // need to iterate over all peers again to build the full uuid_mapping to
          // send to this machine
          for (const auto &peer_metadata2 : shard.peers) {
            uuid_mapping.emplace(peer_metadata2.split_from, peer_metadata2.address.unique_id);
          }

          MG_ASSERT(false, "adding a shard to split :)");
          ret.shards_to_split.push_back(ShardToSplit{
              .split_key = low_key,
              // .old_shard_version = shard.previous_version,
              .new_shard_version = shard.version,
              .uuid_mapping = uuid_mapping,
          });
        } else {
          spdlog::info("5");
          MG_ASSERT(
              !same_machine,
              "failed to properly handle a new Status type in the heartbeat management and shard assignment code");
        }

        if (const auto pending_split = mapped_pending_splits.find({peer_metadata.address.unique_id, shard.version});
            pending_split != mapped_pending_splits.end()) {
          // Now we handle shard split
          if (shard.pending_split) {
            spdlog::info("Coordinator received split request while split is happening!");
            continue;
          }
          MG_ASSERT(false, "beginning split from coordinator perspective :)");
          spdlog::info("Coordinator beginning new split process after receiving a pending split");
          shard.pending_split = msgs::SuggestedSplitInfo{.shard_to_split_uuid = pending_split->first.first,
                                                         .split_key = pending_split->second,
                                                         .shard_version = pending_split->first.second};

          shard.version = GetHlc();
          std::map<boost::uuids::uuid, boost::uuids::uuid> split_mapping = {};
          ShardMetadata duplicated_shard{shard};
          for (auto &peer_metadata3 : duplicated_shard.peers) {
            peer_metadata3.status = Status::PENDING_SPLIT;
            peer_metadata3.split_from = peer_metadata3.address.unique_id;

            const auto new_uuid = NewShardUuid();

            // store new uuid for the right side of each shard
            split_mapping.emplace(peer_metadata3.address.unique_id, new_uuid);

            peer_metadata3.address.unique_id = new_uuid;
          }
          const auto converted_pk = storage::conversions::ConvertPropertyVector(pending_split->second);
          if (high_key) {
            MG_ASSERT(converted_pk < *high_key, "Split point is beyond low key of the next shard");
          }
          label_space.shards.insert({converted_pk, duplicated_shard});
        } else {
          spdlog::info("Coordinator not attempting to split this shard: there is no pending split for {}:{}",
                       peer_metadata.address.unique_id, shard.version);
        }
      }

      if (shard.pending_split.has_value()) {
        // if the split shard has any peers that are CONSENSUS_PARTICIPANT, we can clear our
        // pending split because even if a single peer has split, it means that the split has
        // reached a majority of the replicas.
        bool split_applied_anywhere = false;
        const auto split_key = storage::conversions::ConvertPropertyVector(shard.pending_split->split_key);

        for (const auto &split_peer : label_space.shards.at(split_key).peers) {
          split_applied_anywhere |= split_peer.status == Status::CONSENSUS_PARTICIPANT;
        }

        if (split_applied_anywhere) {
          spdlog::info("now that the shard in label space {} is done splitting, we can clear its pending_split data",
                       label_id);
          shard.pending_split.reset();
        }
      }

      if (!shard_assigned_to_machine && shard.peers.size() < label_space.replication_factor) {
        Address address = storage_manager;

        address.unique_id = NewShardUuid();

        spdlog::info("assigning shard manager to shard");

        ret.shards_to_initialize.push_back(ShardToInitialize{
            .new_shard_version = shard.version,
            .uuid = address.unique_id,
            .label_id = label_id,
            .min_key = low_key,
            .max_key = high_key,
            .schema = schemas[label_id],
            .config = Config{.split =
                                 Config::Split{
                                     .max_shard_vertex_size = label_space.split_threshold,
                                 }},
            .id_to_names = IdToNames(),
        });

        PeerMetadata peer_metadata = {
            .address = address,
            .status = Status::INITIALIZING,
        };

        shard.peers.emplace_back(peer_metadata);
      }
    }
  }

  if (mutated) {
    IncrementShardMapVersion();
  }

  return ret;
}

std::optional<LabelId> ShardMap::InitializeNewLabel(std::string label_name, std::vector<SchemaProperty> schema,
                                                    size_t replication_factor, uint64_t split_threshold,
                                                    Hlc last_shard_map_version) {
  if (shard_map_version != last_shard_map_version || labels.contains(label_name)) {
    MG_ASSERT(false, "failed to InitializeNewLabel");
    return std::nullopt;
  }

  const LabelId label_id = LabelId::FromUint(++max_label_id);

  labels.emplace(std::move(label_name), label_id);

  PrimaryKey initial_key = SchemaToMinKey(schema);
  ShardMetadata empty_shard = {};

  Shards shards = {
      {initial_key, empty_shard},
  };

  LabelSpace label_space{
      .schema = schema,
      .shards = shards,
      .replication_factor = replication_factor,
      .split_threshold = split_threshold,
  };

  schemas[label_id] = std::move(schema);

  label_spaces.emplace(label_id, label_space);

  IncrementShardMapVersion();

  return label_id;
}

void ShardMap::AddServer(Address server_address) {
  // Find a random place for the server to plug in
}

std::optional<LabelId> ShardMap::GetLabelId(const std::string &label) const {
  if (const auto it = labels.find(label); it != labels.end()) {
    return it->second;
  }
  return std::nullopt;
}

const std::string &ShardMap::GetLabelName(const LabelId label) const {
  if (const auto it =
          std::ranges::find_if(labels, [label](const auto &name_id_pair) { return name_id_pair.second == label; });
      it != labels.end()) {
    return it->first;
  }
  throw utils::BasicException("GetLabelName fails on the given label id!");
}

std::optional<PropertyId> ShardMap::GetPropertyId(const std::string &property_name) const {
  if (const auto it = properties.find(property_name); it != properties.end()) {
    return it->second;
  }
  return std::nullopt;
}

const std::string &ShardMap::GetPropertyName(const PropertyId property) const {
  if (const auto it = std::ranges::find_if(
          properties, [property](const auto &name_id_pair) { return name_id_pair.second == property; });
      it != properties.end()) {
    return it->first;
  }
  throw utils::BasicException("PropertyId not found!");
}

std::optional<EdgeTypeId> ShardMap::GetEdgeTypeId(const std::string &edge_type) const {
  if (const auto it = edge_types.find(edge_type); it != edge_types.end()) {
    return it->second;
  }
  return std::nullopt;
}

const std::string &ShardMap::GetEdgeTypeName(const EdgeTypeId property) const {
  if (const auto it = std::ranges::find_if(
          edge_types, [property](const auto &name_id_pair) { return name_id_pair.second == property; });
      it != edge_types.end()) {
    return it->first;
  }
  throw utils::BasicException("EdgeTypeId not found!");
}

Shards ShardMap::GetShardsForRange(const LabelName &label_name, const PrimaryKey &start_key,
                                   const PrimaryKey &end_key) const {
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

ShardMetadata ShardMap::GetShardForKey(const LabelName &label_name, const PrimaryKey &key) const {
  MG_ASSERT(labels.contains(label_name));

  LabelId label_id = labels.at(label_name);

  const auto &label_space = label_spaces.at(label_id);

  MG_ASSERT(label_space.shards.begin()->first <= key,
            "the ShardMap must always contain a minimal key that is less than or equal to any requested key");

  return std::prev(label_space.shards.upper_bound(key))->second;
}

ShardMetadata ShardMap::GetShardForKey(const LabelId &label_id, const PrimaryKey &key) const {
  MG_ASSERT(label_spaces.contains(label_id));

  const auto &label_space = label_spaces.at(label_id);

  MG_ASSERT(label_space.shards.begin()->first <= key,
            "the ShardMap must always contain a minimal key that is less than or equal to any requested key");

  return std::prev(label_space.shards.upper_bound(key))->second;
}

PropertyMap ShardMap::AllocatePropertyIds(const std::vector<PropertyName> &new_properties) {
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

EdgeTypeIdMap ShardMap::AllocateEdgeTypeIds(const std::vector<EdgeTypeName> &new_edge_types) {
  EdgeTypeIdMap ret;

  bool mutated = false;

  for (const auto &edge_type_name : new_edge_types) {
    if (edge_types.contains(edge_type_name)) {
      auto edge_type_id = edge_types.at(edge_type_name);
      ret.emplace(edge_type_name, edge_type_id);
    } else {
      mutated = true;

      const EdgeTypeId edge_type_id = EdgeTypeId::FromUint(++max_edge_type_id);
      ret.emplace(edge_type_name, edge_type_id);
      edge_types.emplace(edge_type_name, edge_type_id);
    }
  }

  if (mutated) {
    IncrementShardMapVersion();
  }

  return ret;
}

bool ShardMap::ClusterInitialized() const {
  for (const auto &[label_id, label_space] : label_spaces) {
    for (const auto &[low_key, shard] : label_space.shards) {
      if (shard.peers.size() < label_space.replication_factor) {
        spdlog::info("label_space below desired replication factor");
        return false;
      }

      for (const auto &peer_metadata : shard.peers) {
        if (peer_metadata.status != Status::CONSENSUS_PARTICIPANT) {
          spdlog::info("shard member not yet a CONSENSUS_PARTICIPANT");
          return false;
        }
      }
    }
  }

  return true;
}

size_t ShardMap::InitializedShards() const {
  size_t count = 0;

  for (const auto &[label_id, label_space] : label_spaces) {
    for (const auto &[low_key, shard] : label_space.shards) {
      if (shard.peers.size() < label_space.replication_factor) {
        continue;
      }

      for (const auto &peer_metadata : shard.peers) {
        if (peer_metadata.status != Status::CONSENSUS_PARTICIPANT) {
          continue;
        }
      }

      count += 1;
    }
  }

  return count;
}

}  // namespace memgraph::coordinator
