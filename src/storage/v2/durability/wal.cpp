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

#include "storage/v2/durability/wal.hpp"

#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::durability {

// WAL format:
//
// 1) Magic string (non-encoded)
//
// 2) WAL version (non-encoded, little-endian)
//
// 3) Section offsets:
//     * offset to the metadata section
//     * offset to the first delta in the WAL
//
// 4) Metadata
//     * storage UUID
//     * sequence number (number indicating the sequence position of this WAL
//       file)
//
// 5) Encoded deltas; each delta is written in the following format:
//     * commit timestamp
//     * action (only one of the actions below are encoded)
//         * vertex create, vertex delete
//              * gid
//         * vertex add label, vertex remove label
//              * gid
//              * label name
//         * vertex set property
//              * gid
//              * property name
//              * property value
//         * edge create, edge delete
//              * gid
//              * edge type name
//              * from vertex gid
//              * to vertex gid
//         * edge set property
//              * gid
//              * property name
//              * property value
//         * transaction end (marks that the whole transaction is
//           stored in the WAL file)
//         * label index create, label index drop
//              * label name
//         * label property index create, label property index drop,
//           existence constraint create, existence constraint drop
//              * label name
//              * property name
//         * unique constraint create, unique constraint drop
//              * label name
//              * property names
//
// IMPORTANT: When changing WAL encoding/decoding bump the snapshot/WAL version
// in `version.hpp`.

namespace {

constexpr Marker OperationToMarker(StorageMetadataOperation operation) {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define add_case(E)                 \
  case StorageMetadataOperation::E: \
    return Marker::DELTA_##E
  switch (operation) {
    add_case(EDGE_INDEX_CREATE);
    add_case(EDGE_INDEX_DROP);
    add_case(EDGE_PROPERTY_INDEX_CREATE);
    add_case(EDGE_PROPERTY_INDEX_DROP);
    add_case(ENUM_ALTER_ADD);
    add_case(ENUM_ALTER_UPDATE);
    add_case(ENUM_CREATE);
    add_case(EXISTENCE_CONSTRAINT_CREATE);
    add_case(EXISTENCE_CONSTRAINT_DROP);
    add_case(LABEL_INDEX_CREATE);
    add_case(LABEL_INDEX_DROP);
    add_case(LABEL_INDEX_STATS_CLEAR);
    add_case(LABEL_INDEX_STATS_SET);
    add_case(LABEL_PROPERTY_INDEX_CREATE);
    add_case(LABEL_PROPERTY_INDEX_DROP);
    add_case(LABEL_PROPERTY_INDEX_STATS_CLEAR);
    add_case(LABEL_PROPERTY_INDEX_STATS_SET);
    add_case(TEXT_INDEX_CREATE);
    add_case(TEXT_INDEX_DROP);
    add_case(UNIQUE_CONSTRAINT_CREATE);
    add_case(UNIQUE_CONSTRAINT_DROP);
    add_case(TYPE_CONSTRAINT_CREATE);
    add_case(TYPE_CONSTRAINT_DROP);
    add_case(POINT_INDEX_CREATE);
    add_case(POINT_INDEX_DROP);
  }
#undef add_case
}

constexpr Marker DeltaActionToMarker(Delta::Action action) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  switch (action) {
    case Delta::Action::DELETE_DESERIALIZED_OBJECT:
    case Delta::Action::DELETE_OBJECT:
      return Marker::DELTA_VERTEX_CREATE;
    case Delta::Action::RECREATE_OBJECT:
      return Marker::DELTA_VERTEX_DELETE;
    case Delta::Action::SET_PROPERTY:
      return Marker::DELTA_VERTEX_SET_PROPERTY;
    case Delta::Action::ADD_LABEL:
      return Marker::DELTA_VERTEX_REMOVE_LABEL;
    case Delta::Action::REMOVE_LABEL:
      return Marker::DELTA_VERTEX_ADD_LABEL;
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::ADD_OUT_EDGE:
      return Marker::DELTA_EDGE_DELETE;
    case Delta::Action::REMOVE_IN_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE:
      return Marker::DELTA_EDGE_CREATE;
  }
}

// This function convertes a Marker to a WalDeltaData::Type. It checks for the
// validity of the marker and throws if an invalid marker is specified.
// @throw RecoveryFailure
constexpr WalDeltaData::Type MarkerToWalDeltaDataType(Marker marker) {
  // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define add_case(E)       \
  case Marker::DELTA_##E: \
    return WalDeltaData::Type::E
  switch (marker) {
    add_case(EDGE_CREATE);
    add_case(EDGE_DELETE);
    add_case(EDGE_SET_PROPERTY);
    add_case(EDGE_INDEX_CREATE);
    add_case(EDGE_INDEX_DROP);
    add_case(EDGE_PROPERTY_INDEX_CREATE);
    add_case(EDGE_PROPERTY_INDEX_DROP);
    add_case(ENUM_ALTER_ADD);
    add_case(ENUM_ALTER_UPDATE);
    add_case(ENUM_CREATE);
    add_case(EXISTENCE_CONSTRAINT_CREATE);
    add_case(EXISTENCE_CONSTRAINT_DROP);
    add_case(LABEL_INDEX_CREATE);
    add_case(LABEL_INDEX_DROP);
    add_case(LABEL_INDEX_STATS_CLEAR);
    add_case(LABEL_INDEX_STATS_SET);
    add_case(LABEL_PROPERTY_INDEX_CREATE);
    add_case(LABEL_PROPERTY_INDEX_DROP);
    add_case(LABEL_PROPERTY_INDEX_STATS_CLEAR);
    add_case(LABEL_PROPERTY_INDEX_STATS_SET);
    add_case(TEXT_INDEX_CREATE);
    add_case(TEXT_INDEX_DROP);
    add_case(TRANSACTION_END);
    add_case(UNIQUE_CONSTRAINT_CREATE);
    add_case(UNIQUE_CONSTRAINT_DROP);
    add_case(TYPE_CONSTRAINT_CREATE);
    add_case(TYPE_CONSTRAINT_DROP);
    add_case(VERTEX_ADD_LABEL);
    add_case(VERTEX_CREATE);
    add_case(VERTEX_DELETE);
    add_case(VERTEX_REMOVE_LABEL);
    add_case(VERTEX_SET_PROPERTY);
    add_case(POINT_INDEX_CREATE);
    add_case(POINT_INDEX_DROP);

    case Marker::TYPE_NULL:
    case Marker::TYPE_BOOL:
    case Marker::TYPE_INT:
    case Marker::TYPE_DOUBLE:
    case Marker::TYPE_STRING:
    case Marker::TYPE_LIST:
    case Marker::TYPE_MAP:
    case Marker::TYPE_TEMPORAL_DATA:
    case Marker::TYPE_ZONED_TEMPORAL_DATA:
    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::TYPE_ENUM:
    case Marker::TYPE_POINT_2D:
    case Marker::TYPE_POINT_3D:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_EPOCH_HISTORY:
    case Marker::SECTION_EDGE_INDICES:
    case Marker::SECTION_OFFSETS:
    case Marker::SECTION_ENUMS:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      throw RecoveryFailure("Invalid WAL data!");
  }
#undef add_case
}

// Function used to either read or skip the current WAL delta data. The WAL
// delta header must be read before calling this function. If the delta data is
// read then the data returned is valid, if the delta data is skipped then the
// returned data is not guaranteed to be set (it could be empty) and shouldn't
// be used.
// @throw RecoveryFailure
template <bool read_data>
WalDeltaData ReadSkipWalDeltaData(BaseDecoder *decoder) {
  WalDeltaData delta;

  auto action = decoder->ReadMarker();
  if (!action) throw RecoveryFailure("Invalid WAL data!");
  delta.type = MarkerToWalDeltaDataType(*action);

  switch (delta.type) {
    case WalDeltaData::Type::VERTEX_CREATE:
    case WalDeltaData::Type::VERTEX_DELETE: {
      auto gid = decoder->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_create_delete.gid = Gid::FromUint(*gid);
      break;
    }
    case WalDeltaData::Type::VERTEX_ADD_LABEL:
    case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
      auto gid = decoder->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_add_remove_label.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_add_remove_label.label = std::move(*label);
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::VERTEX_SET_PROPERTY:
    case WalDeltaData::Type::EDGE_SET_PROPERTY: {
      auto gid = decoder->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_edge_set_property.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto property = decoder->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_edge_set_property.property = std::move(*property);
        auto value = decoder->ReadPropertyValue();
        if (!value) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_edge_set_property.value = std::move(*value);
      } else {
        if (!decoder->SkipString() || !decoder->SkipPropertyValue()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::EDGE_CREATE:
    case WalDeltaData::Type::EDGE_DELETE: {
      auto gid = decoder->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto edge_type = decoder->ReadString();
        if (!edge_type) throw RecoveryFailure("Invalid WAL data!");
        delta.edge_create_delete.edge_type = std::move(*edge_type);
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      auto from_gid = decoder->ReadUint();
      if (!from_gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.from_vertex = Gid::FromUint(*from_gid);
      auto to_gid = decoder->ReadUint();
      if (!to_gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.to_vertex = Gid::FromUint(*to_gid);
      break;
    }
    case WalDeltaData::Type::TRANSACTION_END:
      break;
    // NOLINTNEXTLINE(bugprone-branch-clone)
    case WalDeltaData::Type::LABEL_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_INDEX_DROP:
    case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: /* Special case, this clear is done on all label/property
                                                                  pairs that contain the defined label */
    {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label.label = std::move(*label);
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::EDGE_INDEX_CREATE:
    case WalDeltaData::Type::EDGE_INDEX_DROP: {
      if constexpr (read_data) {
        auto edge_type = decoder->ReadString();
        if (!edge_type) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_edge_type.edge_type = std::move(*edge_type);
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::LABEL_INDEX_STATS_SET: {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_stats.label = std::move(*label);
        auto stats = decoder->ReadString();
        if (!stats) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_stats.stats = std::move(*stats);
      } else {
        if (!decoder->SkipString() || !decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
    } break;
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::POINT_INDEX_CREATE:
    case WalDeltaData::Type::POINT_INDEX_DROP:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property.label = std::move(*label);
        auto property = decoder->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property.property = std::move(*property);
      } else {
        if (!decoder->SkipString() || !decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET: {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_stats.label = std::move(*label);
        auto property = decoder->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_stats.property = std::move(*property);
        auto stats = decoder->ReadString();
        if (!stats) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_stats.stats = std::move(*stats);
      } else {
        if (!decoder->SkipString() || !decoder->SkipString() || !decoder->SkipString())
          throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::EDGE_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::EDGE_PROPERTY_INDEX_DROP: {
      if constexpr (read_data) {
        auto edge_type = decoder->ReadString();
        if (!edge_type) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_edge_type_property.edge_type = std::move(*edge_type);
        auto property = decoder->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_edge_type_property.property = std::move(*property);
      } else {
        // Skips the edge type and property strings
        if (!decoder->SkipString() || !decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_properties.label = std::move(*label);
        auto properties_count = decoder->ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid WAL data!");
        for (uint64_t i = 0; i < *properties_count; ++i) {
          auto property = decoder->ReadString();
          if (!property) throw RecoveryFailure("Invalid WAL data!");
          delta.operation_label_properties.properties.emplace(std::move(*property));
        }
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        auto properties_count = decoder->ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid WAL data!");
        for (uint64_t i = 0; i < *properties_count; ++i) {
          if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        }
      }
      break;
    }
    case WalDeltaData::Type::TYPE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::TYPE_CONSTRAINT_DROP: {
      if constexpr (read_data) {
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_type.label = std::move(*label);

        auto property = decoder->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_type.property = std::move(*property);

        auto type = decoder->ReadUint();
        if (!type) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property_type.type = static_cast<TypeConstraintKind>(*type);
      } else {
        if (!decoder->SkipString() || !decoder->SkipString() || !decoder->ReadUint())
          throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::TEXT_INDEX_CREATE:
    case WalDeltaData::Type::TEXT_INDEX_DROP: {
      if constexpr (read_data) {
        auto index_name = decoder->ReadString();
        if (!index_name) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_text.index_name = std::move(*index_name);
        auto label = decoder->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_text.label = std::move(*label);
      } else {
        if (!decoder->SkipString() || !decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::ENUM_CREATE: {
      if constexpr (read_data) {
        auto etype = decoder->ReadString();
        if (!etype) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_create.etype = *etype;

        auto evalues_count = decoder->ReadUint();
        if (!evalues_count) throw RecoveryFailure("Invalid WAL data!");
        auto evalues = std::vector<std::string>{};
        evalues.reserve(*evalues_count);
        for (auto i = 0; i != *evalues_count; ++i) {
          auto evalue = decoder->ReadString();
          if (!evalue) throw RecoveryFailure("Invalid WAL data!");
          evalues.emplace_back(*std::move(evalue));
        }
        delta.operation_enum_create.evalues = std::move(evalues);
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");

        auto evalues_count = decoder->ReadUint();
        if (!evalues_count) throw RecoveryFailure("Invalid WAL data!");
        for (auto i = 0; i != *evalues_count; ++i) {
          if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        }
      }
      break;
    }
    case WalDeltaData::Type::ENUM_ALTER_ADD:
      if constexpr (read_data) {
        auto etype = decoder->ReadString();
        if (!etype) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_alter_add.etype = *etype;

        auto evalue = decoder->ReadString();
        if (!evalue) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_alter_add.evalue = *evalue;
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    case WalDeltaData::Type::ENUM_ALTER_UPDATE:
      if constexpr (read_data) {
        auto etype = decoder->ReadString();
        if (!etype) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_alter_update.etype = *etype;

        auto evalue_old = decoder->ReadString();
        if (!evalue_old) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_alter_update.evalue_old = *evalue_old;

        auto evalue_new = decoder->ReadString();
        if (!evalue_new) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_enum_alter_update.evalue_new = *evalue_new;
      } else {
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        if (!decoder->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
  }

  return delta;
}

}  // namespace

// Function used to read information about the WAL file.
WalInfo ReadWalInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version) throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure("Invalid WAL version!");

  // Prepare return value.
  WalInfo info;

  // Read offsets.
  {
    auto marker = wal.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS) throw RecoveryFailure("Invalid WAL data!");

    auto wal_size = wal.GetSize();
    if (!wal_size) throw RecoveryFailure("Invalid WAL data!");

    auto read_offset = [&wal, wal_size] {
      auto maybe_offset = wal.ReadUint();
      if (!maybe_offset) throw RecoveryFailure("Invalid WAL format!");
      auto offset = *maybe_offset;
      if (offset > *wal_size) throw RecoveryFailure("Invalid WAL format!");
      return offset;
    };

    info.offset_metadata = read_offset();
    info.offset_deltas = read_offset();
  }

  // Read metadata.
  {
    wal.SetPosition(info.offset_metadata);

    auto marker = wal.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA) throw RecoveryFailure("Invalid WAL data!");

    auto maybe_uuid = wal.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Invalid WAL data!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_epoch_id = wal.ReadString();
    if (!maybe_epoch_id) throw RecoveryFailure("Invalid WAL data!");
    info.epoch_id = std::move(*maybe_epoch_id);

    auto maybe_seq_num = wal.ReadUint();
    if (!maybe_seq_num) throw RecoveryFailure("Invalid WAL data!");
    info.seq_num = *maybe_seq_num;
  }

  // Read deltas.
  info.num_deltas = 0;
  auto validate_delta = [&wal, version = *version]() -> std::optional<std::pair<uint64_t, bool>> {
    try {
      auto timestamp = ReadWalDeltaHeader(&wal);
      auto type = SkipWalDeltaData(&wal);
      return {{timestamp, IsWalDeltaDataTypeTransactionEnd(type, version)}};
    } catch (const RecoveryFailure &) {
      return std::nullopt;
    }
  };
  auto size = wal.GetSize();
  // Here we read the whole file and determine the number of valid deltas. A
  // delta is valid only if all of its data can be successfully read. This
  // allows us to recover data from WAL files that are corrupt at the end (eg.
  // because of power loss) but are still valid at the beginning. While reading
  // the deltas we only count deltas which are a part of a fully valid
  // transaction (indicated by a TRANSACTION_END delta or any other
  // non-transactional operation).
  std::optional<uint64_t> current_timestamp;
  uint64_t num_deltas = 0;
  while (wal.GetPosition() != size) {
    auto ret = validate_delta();
    if (!ret) break;
    auto [timestamp, is_end_of_transaction] = *ret;
    if (!current_timestamp) current_timestamp = timestamp;
    if (*current_timestamp != timestamp) break;
    ++num_deltas;
    if (is_end_of_transaction) {
      if (info.num_deltas == 0) {
        info.from_timestamp = timestamp;
        info.to_timestamp = timestamp;
      }
      if (timestamp < info.from_timestamp || timestamp < info.to_timestamp) break;
      info.to_timestamp = timestamp;
      info.num_deltas += num_deltas;
      current_timestamp = std::nullopt;
      num_deltas = 0;
    }
  }

  if (info.num_deltas == 0) throw RecoveryFailure("Invalid WAL data!");

  return info;
}

bool operator==(const WalDeltaData &a, const WalDeltaData &b) {
  if (a.type != b.type) return false;
  switch (a.type) {
    case WalDeltaData::Type::VERTEX_CREATE:
    case WalDeltaData::Type::VERTEX_DELETE:
      return a.vertex_create_delete.gid == b.vertex_create_delete.gid;

    case WalDeltaData::Type::VERTEX_ADD_LABEL:
    case WalDeltaData::Type::VERTEX_REMOVE_LABEL:
      return a.vertex_add_remove_label.gid == b.vertex_add_remove_label.gid &&
             a.vertex_add_remove_label.label == b.vertex_add_remove_label.label;

    case WalDeltaData::Type::VERTEX_SET_PROPERTY:
    case WalDeltaData::Type::EDGE_SET_PROPERTY:
      return a.vertex_edge_set_property.gid == b.vertex_edge_set_property.gid &&
             a.vertex_edge_set_property.property == b.vertex_edge_set_property.property &&
             a.vertex_edge_set_property.value == b.vertex_edge_set_property.value;

    case WalDeltaData::Type::EDGE_CREATE:
    case WalDeltaData::Type::EDGE_DELETE:
      return a.edge_create_delete.gid == b.edge_create_delete.gid &&
             a.edge_create_delete.edge_type == b.edge_create_delete.edge_type &&
             a.edge_create_delete.from_vertex == b.edge_create_delete.from_vertex &&
             a.edge_create_delete.to_vertex == b.edge_create_delete.to_vertex;

    case WalDeltaData::Type::TRANSACTION_END:
      return true;

    case WalDeltaData::Type::LABEL_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_INDEX_DROP:
    case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: /* Special case, label property index stats clear just
                                                                  passes the label and all label/property pairs with the
                                                                  label get cleared */
      return a.operation_label.label == b.operation_label.label;

    case WalDeltaData::Type::LABEL_INDEX_STATS_SET:
      return a.operation_label_stats.label == b.operation_label_stats.label &&
             a.operation_label_stats.stats == b.operation_label_stats.stats;

    case WalDeltaData::Type::TEXT_INDEX_CREATE:
    case WalDeltaData::Type::TEXT_INDEX_DROP:
      return a.operation_text.index_name == b.operation_text.index_name &&
             a.operation_text.label == b.operation_text.label;

    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::POINT_INDEX_CREATE:
    case WalDeltaData::Type::POINT_INDEX_DROP:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP:
      return a.operation_label_property.label == b.operation_label_property.label &&
             a.operation_label_property.property == b.operation_label_property.property;

    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET:
      return a.operation_label_property_stats.label == b.operation_label_property_stats.label &&
             a.operation_label_property_stats.property == b.operation_label_property_stats.property &&
             a.operation_label_property_stats.stats == b.operation_label_property_stats.stats;

    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP:
      return a.operation_label_properties.label == b.operation_label_properties.label &&
             a.operation_label_properties.properties == b.operation_label_properties.properties;
    case WalDeltaData::Type::TYPE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::TYPE_CONSTRAINT_DROP:
      return a.operation_label_property_type.label == b.operation_label_property_type.label &&
             a.operation_label_property_type.property == b.operation_label_property_type.property &&
             a.operation_label_property_type.type == b.operation_label_property_type.type;
    case WalDeltaData::Type::EDGE_INDEX_CREATE:
    case WalDeltaData::Type::EDGE_INDEX_DROP:
      return a.operation_edge_type.edge_type == b.operation_edge_type.edge_type;
    case WalDeltaData::Type::EDGE_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::EDGE_PROPERTY_INDEX_DROP:
      return a.operation_edge_type_property.edge_type == b.operation_edge_type_property.edge_type &&
             a.operation_edge_type_property.property == b.operation_edge_type_property.property;
    case WalDeltaData::Type::ENUM_CREATE: {
      return std::tie(a.operation_enum_create.etype, a.operation_enum_create.evalues) ==
             std::tie(b.operation_enum_create.etype, b.operation_enum_create.evalues);
    }
    case WalDeltaData::Type::ENUM_ALTER_ADD: {
      return std::tie(a.operation_enum_alter_add.etype, a.operation_enum_alter_add.evalue) ==
             std::tie(b.operation_enum_alter_add.etype, b.operation_enum_alter_add.evalue);
    }
    case WalDeltaData::Type::ENUM_ALTER_UPDATE: {
      return std::tie(a.operation_enum_alter_update.etype, a.operation_enum_alter_update.evalue_old,
                      a.operation_enum_alter_update.evalue_new) == std::tie(b.operation_enum_alter_update.etype,
                                                                            b.operation_enum_alter_update.evalue_old,
                                                                            b.operation_enum_alter_update.evalue_new);
    }
  }
}
bool operator!=(const WalDeltaData &a, const WalDeltaData &b) { return !(a == b); }

// Function used to read the WAL delta header. The function returns the delta
// timestamp.
uint64_t ReadWalDeltaHeader(BaseDecoder *decoder) {
  auto marker = decoder->ReadMarker();
  if (!marker || *marker != Marker::SECTION_DELTA) throw RecoveryFailure("Invalid WAL data!");

  auto timestamp = decoder->ReadUint();
  if (!timestamp) throw RecoveryFailure("Invalid WAL data!");
  return *timestamp;
}

// Function used to read the current WAL delta data. The WAL delta header must
// be read before calling this function.
WalDeltaData ReadWalDeltaData(BaseDecoder *decoder) { return ReadSkipWalDeltaData<true>(decoder); }

// Function used to skip the current WAL delta data. The WAL delta header must
// be read before calling this function.
WalDeltaData::Type SkipWalDeltaData(BaseDecoder *decoder) {
  auto delta = ReadSkipWalDeltaData<false>(decoder);
  return delta.type;
}

void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, SalientConfig::Items items, const Delta &delta,
                 const Vertex &vertex, uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  encoder->WriteMarker(Marker::SECTION_DELTA);
  encoder->WriteUint(timestamp);
  auto guard = std::shared_lock{vertex.lock};
  switch (delta.action) {
    case Delta::Action::DELETE_DESERIALIZED_OBJECT:
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT: {
      encoder->WriteMarker(DeltaActionToMarker(delta.action));
      encoder->WriteUint(vertex.gid.AsUint());
      break;
    }
    case Delta::Action::SET_PROPERTY: {
      encoder->WriteMarker(Marker::DELTA_VERTEX_SET_PROPERTY);
      encoder->WriteUint(vertex.gid.AsUint());
      encoder->WriteString(name_id_mapper->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // vertex.
      // TODO (mferencevic): Mitigate the memory allocation introduced here
      // (with the `GetProperty` call). It is the only memory allocation in the
      // entire WAL file writing logic.
      encoder->WritePropertyValue(vertex.properties.GetProperty(delta.property.key));
      break;
    }
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL: {
      encoder->WriteMarker(DeltaActionToMarker(delta.action));
      encoder->WriteUint(vertex.gid.AsUint());
      encoder->WriteString(name_id_mapper->IdToName(delta.label.value.AsUint()));
      break;
    }
    case Delta::Action::ADD_OUT_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE: {
      encoder->WriteMarker(DeltaActionToMarker(delta.action));
      if (items.properties_on_edges) {
        encoder->WriteUint(delta.vertex_edge.edge.ptr->gid.AsUint());
      } else {
        encoder->WriteUint(delta.vertex_edge.edge.gid.AsUint());
      }
      encoder->WriteString(name_id_mapper->IdToName(delta.vertex_edge.edge_type.AsUint()));
      encoder->WriteUint(vertex.gid.AsUint());
      encoder->WriteUint(delta.vertex_edge.vertex->gid.AsUint());
      break;
    }
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::REMOVE_IN_EDGE:
      // These actions are already encoded in the *_OUT_EDGE actions. This
      // function should never be called for this type of deltas.
      LOG_FATAL("Invalid delta action!");
  }
}

void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, const Delta &delta, const Edge &edge,
                 uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  encoder->WriteMarker(Marker::SECTION_DELTA);
  encoder->WriteUint(timestamp);
  auto guard = std::shared_lock{edge.lock};
  switch (delta.action) {
    case Delta::Action::SET_PROPERTY: {
      encoder->WriteMarker(Marker::DELTA_EDGE_SET_PROPERTY);
      encoder->WriteUint(edge.gid.AsUint());
      encoder->WriteString(name_id_mapper->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // edge.
      // TODO (mferencevic): Mitigate the memory allocation introduced here
      // (with the `GetProperty` call). It is the only memory allocation in the
      // entire WAL file writing logic.
      encoder->WritePropertyValue(edge.properties.GetProperty(delta.property.key));
      break;
    }
    case Delta::Action::DELETE_DESERIALIZED_OBJECT:
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT:
      // These actions are already encoded in vertex *_OUT_EDGE actions. Also,
      // these deltas don't contain any information about the from vertex, to
      // vertex or edge type so they are useless. This function should never
      // be called for this type of deltas.
      LOG_FATAL("Invalid delta action!");
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL:
    case Delta::Action::ADD_OUT_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE:
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::REMOVE_IN_EDGE:
      // These deltas shouldn't appear for edges.
      LOG_FATAL("Invalid database state!");
  }
}

void EncodeTransactionEnd(BaseEncoder *encoder, uint64_t timestamp) {
  encoder->WriteMarker(Marker::SECTION_DELTA);
  encoder->WriteUint(timestamp);
  encoder->WriteMarker(Marker::DELTA_TRANSACTION_END);
}

RecoveryInfo LoadWal(const std::filesystem::path &path, RecoveredIndicesAndConstraints *indices_constraints,
                     const std::optional<uint64_t> last_loaded_timestamp, utils::SkipList<Vertex> *vertices,
                     utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                     SalientConfig::Items items, EnumStore *enum_store, SchemaInfo *schema_info,
                     std::function<std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>(Gid)> find_edge) {
  spdlog::info("Trying to load WAL file {}.", path);
  RecoveryInfo ret;

  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version) throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure("Invalid WAL version!");

  // Read wal info.
  auto info = ReadWalInfo(path);
  ret.last_durable_timestamp = info.to_timestamp;

  // Check timestamp.
  if (last_loaded_timestamp && info.to_timestamp <= *last_loaded_timestamp) {
    spdlog::info("Skip loading WAL file because it is too old.");
    return ret;
  }

  // Recover deltas.
  wal.SetPosition(info.offset_deltas);
  uint64_t deltas_applied = 0;
  auto edge_acc = edges->access();
  auto vertex_acc = vertices->access();
  spdlog::info("WAL file contains {} deltas.", info.num_deltas);
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    // Read WAL delta header to find out the delta timestamp.
    auto timestamp = ReadWalDeltaHeader(&wal);

    if (!last_loaded_timestamp || timestamp > *last_loaded_timestamp) {
      // This delta should be loaded.
      auto delta = ReadWalDeltaData(&wal);
      switch (delta.type) {
        case WalDeltaData::Type::VERTEX_CREATE: {
          auto [vertex, inserted] = vertex_acc.insert(Vertex{delta.vertex_create_delete.gid, nullptr});
          if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

          ret.next_vertex_id = std::max(ret.next_vertex_id, delta.vertex_create_delete.gid.AsUint() + 1);

          if (schema_info) schema_info->AddVertex(&*vertex);
          break;
        }
        case WalDeltaData::Type::VERTEX_DELETE: {
          auto vertex = vertex_acc.find(delta.vertex_create_delete.gid);
          if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");
          if (!vertex->in_edges.empty() || !vertex->out_edges.empty())
            throw RecoveryFailure("The vertex can't be deleted because it still has edges!");

          if (!vertex_acc.remove(delta.vertex_create_delete.gid))
            throw RecoveryFailure("The vertex must be removed here!");

          if (schema_info) schema_info->DeleteVertex(&*vertex);
          break;
        }
        case WalDeltaData::Type::VERTEX_ADD_LABEL:
        case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
          auto vertex = vertex_acc.find(delta.vertex_add_remove_label.gid);
          if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");

          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.vertex_add_remove_label.label));
          auto it = std::find(vertex->labels.begin(), vertex->labels.end(), label_id);

          std::optional<utils::small_vector<LabelId>> old_labels{};
          if (schema_info) old_labels.emplace(vertex->labels);

          if (delta.type == WalDeltaData::Type::VERTEX_ADD_LABEL) {
            if (it != vertex->labels.end()) throw RecoveryFailure("The vertex already has the label!");
            vertex->labels.push_back(label_id);
          } else {
            if (it == vertex->labels.end()) throw RecoveryFailure("The vertex doesn't have the label!");
            std::swap(*it, vertex->labels.back());
            vertex->labels.pop_back();
          }

          if (schema_info) schema_info->UpdateLabels(&*vertex, *old_labels, vertex->labels, items.properties_on_edges);
          break;
        }
        case WalDeltaData::Type::VERTEX_SET_PROPERTY: {
          auto vertex = vertex_acc.find(delta.vertex_edge_set_property.gid);
          if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");

          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;

          if (schema_info) {
            const auto old_type = vertex->properties.GetExtendedPropertyType(property_id);
            schema_info->SetProperty(&*vertex, property_id, ExtendedPropertyType{property_value}, old_type);
          }

          vertex->properties.SetProperty(property_id, property_value);
          break;
        }
        case WalDeltaData::Type::EDGE_CREATE: {
          auto from_vertex = vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end()) throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end()) throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items.properties_on_edges) {
            auto [edge, inserted] = edge_acc.insert(Edge{edge_gid, nullptr});
            if (!inserted) throw RecoveryFailure("The edge must be inserted here!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(), from_vertex->out_edges.end(), link);
            if (it != from_vertex->out_edges.end()) throw RecoveryFailure("The from vertex already has this edge!");
            from_vertex->out_edges.push_back(link);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(), to_vertex->in_edges.end(), link);
            if (it != to_vertex->in_edges.end()) throw RecoveryFailure("The to vertex already has this edge!");
            to_vertex->in_edges.push_back(link);
          }

          ret.next_edge_id = std::max(ret.next_edge_id, edge_gid.AsUint() + 1);

          // Increment edge count.
          edge_count->fetch_add(1, std::memory_order_acq_rel);

          if (schema_info) schema_info->CreateEdge(&*from_vertex, &*to_vertex, edge_type_id);
          break;
        }
        case WalDeltaData::Type::EDGE_DELETE: {
          auto from_vertex = vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end()) throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end()) throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items.properties_on_edges) {
            auto edge = edge_acc.find(edge_gid);
            if (edge == edge_acc.end()) throw RecoveryFailure("The edge doesn't exist!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(), from_vertex->out_edges.end(), link);
            if (it == from_vertex->out_edges.end()) throw RecoveryFailure("The from vertex doesn't have this edge!");
            std::swap(*it, from_vertex->out_edges.back());
            from_vertex->out_edges.pop_back();
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(), to_vertex->in_edges.end(), link);
            if (it == to_vertex->in_edges.end()) throw RecoveryFailure("The to vertex doesn't have this edge!");
            std::swap(*it, to_vertex->in_edges.back());
            to_vertex->in_edges.pop_back();
          }
          if (items.properties_on_edges) {
            if (!edge_acc.remove(edge_gid)) throw RecoveryFailure("The edge must be removed here!");
          }

          // Decrement edge count.
          edge_count->fetch_add(-1, std::memory_order_acq_rel);

          if (schema_info)
            schema_info->DeleteEdge(edge_type_id, edge_ref, &*from_vertex, &*to_vertex, items.properties_on_edges);
          break;
        }
        case WalDeltaData::Type::EDGE_SET_PROPERTY: {
          if (!items.properties_on_edges)
            throw RecoveryFailure(
                "The WAL has properties on edges, but the storage is "
                "configured without properties on edges!");
          auto edge = edge_acc.find(delta.vertex_edge_set_property.gid);
          if (edge == edge_acc.end()) throw RecoveryFailure("The edge doesn't exist!");
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;

          // TODO Add edge set property delta to WAL
          if (schema_info) {
            const auto old_type = edge->properties.GetExtendedPropertyType(property_id);
            const auto maybe_edge = find_edge(edge->gid);
            if (!maybe_edge) throw RecoveryFailure("Recovery failed, edge not found.");
            const auto &[edge_ref, edge_type, from, to] = *maybe_edge;
            schema_info->SetProperty(edge_type, from, to, property_id, ExtendedPropertyType{property_value}, old_type,
                                     items.properties_on_edges);
          }

          edge->properties.SetProperty(property_id, property_value);
          break;
        }
        case WalDeltaData::Type::TRANSACTION_END: {
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label.label));
          AddRecoveredIndexConstraint(&indices_constraints->indices.label, label_id, "The label index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label.label));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.label, label_id,
                                         "The label index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::EDGE_INDEX_CREATE: {
          auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type.edge_type));
          AddRecoveredIndexConstraint(&indices_constraints->indices.edge, edge_type_id,
                                      "The edge-type index already exists!");
          break;
        }
        case WalDeltaData::Type::EDGE_INDEX_DROP: {
          auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type.edge_type));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.edge, edge_type_id,
                                         "The edge-type index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::EDGE_PROPERTY_INDEX_CREATE: {
          auto edge_type_id =
              EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type_property.edge_type));
          auto property_id =
              PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type_property.property));
          AddRecoveredIndexConstraint(&indices_constraints->indices.edge_property, {edge_type_id, property_id},
                                      "The edge-type + property index already exists!");
          break;
        }
        case WalDeltaData::Type::EDGE_PROPERTY_INDEX_DROP: {
          auto edge_type_id =
              EdgeTypeId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type_property.edge_type));
          auto property_id =
              PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_edge_type_property.property));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.edge_property, {edge_type_id, property_id},
                                         "The edge-type + property index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_STATS_SET: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_stats.label));
          LabelIndexStats stats{};
          if (!FromJson(delta.operation_label_stats.stats, stats)) {
            throw RecoveryFailure("Failed to read statistics!");
          }
          indices_constraints->indices.label_stats.emplace_back(label_id, stats);
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label.label));
          RemoveRecoveredIndexStats(&indices_constraints->indices.label_stats, label_id,
                                    "The label index stats doesn't exist!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          AddRecoveredIndexConstraint(&indices_constraints->indices.label_property, {label_id, property_id},
                                      "The label property index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.label_property, {label_id, property_id},
                                         "The label property index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::POINT_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          AddRecoveredIndexConstraint(&indices_constraints->indices.point_label_property, {label_id, property_id},
                                      "The label property index already exists!");
          break;
        }
        case WalDeltaData::Type::POINT_INDEX_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.point_label_property, {label_id, property_id},
                                         "The label property index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET: {
          auto &info = delta.operation_label_property_stats;
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(info.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(info.property));
          LabelPropertyIndexStats stats{};
          if (!FromJson(info.stats, stats)) {
            throw RecoveryFailure("Failed to read statistics!");
          }
          indices_constraints->indices.label_property_stats.emplace_back(label_id, std::make_pair(property_id, stats));
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label.label));
          RemoveRecoveredIndexStats(&indices_constraints->indices.label_property_stats, label_id,
                                    "The label index stats doesn't exist!");
          break;
        }
        case WalDeltaData::Type::TEXT_INDEX_CREATE: {
          auto index_name = delta.operation_text.index_name;
          auto label = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_text.label));
          AddRecoveredIndexConstraint(&indices_constraints->indices.text_indices, {index_name, label},
                                      "The text index already exists!");
          break;
        }
        case WalDeltaData::Type::TEXT_INDEX_DROP: {
          auto index_name = delta.operation_text.index_name;
          auto label = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_text.label));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.text_indices, {index_name, label},
                                         "The text index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          AddRecoveredIndexConstraint(&indices_constraints->constraints.existence, {label_id, property_id},
                                      "The existence constraint already exists!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(&indices_constraints->constraints.existence, {label_id, property_id},
                                         "The existence constraint doesn't exist!");
          break;
        }
        case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_properties.label));
          std::set<PropertyId> property_ids;
          for (const auto &prop : delta.operation_label_properties.properties) {
            property_ids.insert(PropertyId::FromUint(name_id_mapper->NameToId(prop)));
          }
          AddRecoveredIndexConstraint(&indices_constraints->constraints.unique, {label_id, property_ids},
                                      "The unique constraint already exists!");
          break;
        }
        case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
          auto label_id = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_properties.label));
          std::set<PropertyId> property_ids;
          for (const auto &prop : delta.operation_label_properties.properties) {
            property_ids.insert(PropertyId::FromUint(name_id_mapper->NameToId(prop)));
          }
          RemoveRecoveredIndexConstraint(&indices_constraints->constraints.unique, {label_id, property_ids},
                                         "The unique constraint doesn't exist!");
          break;
        }
        case WalDeltaData::Type::TYPE_CONSTRAINT_CREATE: {
          auto label = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property_type.label));
          auto property = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property_type.property));
          auto type = static_cast<TypeConstraintKind>(delta.operation_label_property_type.type);
          AddRecoveredIndexConstraint(&indices_constraints->constraints.type, {label, property, type},
                                      "The type constraint already exists!");
          break;
        }
        case WalDeltaData::Type::TYPE_CONSTRAINT_DROP: {
          auto label = LabelId::FromUint(name_id_mapper->NameToId(delta.operation_label_property_type.label));
          auto property = PropertyId::FromUint(name_id_mapper->NameToId(delta.operation_label_property_type.property));
          auto type = static_cast<TypeConstraintKind>(delta.operation_label_property_type.type);
          RemoveRecoveredIndexConstraint(&indices_constraints->constraints.type, {label, property, type},
                                         "The type constraint doesn't exist!");
          break;
        }
        case WalDeltaData::Type::ENUM_CREATE: {
          auto res = enum_store->RegisterEnum(delta.operation_enum_create.etype, delta.operation_enum_create.evalues);
          if (res.HasError()) {
            switch (res.GetError()) {
              case EnumStorageError::EnumExists:
                throw RecoveryFailure("The enum already exist!");
              case EnumStorageError::InvalidValue:
                throw RecoveryFailure("The enum has invalid values!");
              default:
                // Should not happen
                throw RecoveryFailure("The enum could not be registered!");
            }
          }
          break;
        }
        case WalDeltaData::Type::ENUM_ALTER_ADD: {
          auto res = enum_store->AddValue(delta.operation_enum_alter_add.etype, delta.operation_enum_alter_add.evalue);
          if (res.HasError()) {
            switch (res.GetError()) {
              case storage::EnumStorageError::InvalidValue:
                throw RecoveryFailure("Enum value already exists.");
              case storage::EnumStorageError::UnknownEnumType:
                throw RecoveryFailure("Unknown Enum type.");
              default:
                // Should not happen
                throw RecoveryFailure("Enum could not be altered.");
            }
          }
          break;
        }
        case WalDeltaData::Type::ENUM_ALTER_UPDATE: {
          auto const &[enum_name, enum_value_old, enum_value_new] = delta.operation_enum_alter_update;
          auto res = enum_store->UpdateValue(enum_name, enum_value_old, enum_value_new);
          if (res.HasError()) {
            switch (res.GetError()) {
              case storage::EnumStorageError::InvalidValue:
                throw RecoveryFailure("Enum value {}::{} already exists.", enum_name, enum_value_new);
              case storage::EnumStorageError::UnknownEnumType:
                throw RecoveryFailure("Unknown Enum name {}.", enum_name);
              case storage::EnumStorageError::UnknownEnumValue:
                throw RecoveryFailure("Unknown Enum value {}::{}.", enum_name, enum_value_old);
              default:
                // Should not happen
                throw RecoveryFailure("Enum could not be altered.");
            }
          }
          break;
        }
      }
      ret.next_timestamp = std::max(ret.next_timestamp, timestamp + 1);
      ++deltas_applied;
    } else {
      // This delta should be skipped.
      SkipWalDeltaData(&wal);
    }
  }

  spdlog::info("Applied {} deltas from WAL. Skipped {} deltas, because they were too old.", deltas_applied,
               info.num_deltas - deltas_applied);

  return ret;
}

WalFile::WalFile(const std::filesystem::path &wal_directory, const std::string_view uuid,
                 const std::string_view epoch_id, SalientConfig::Items items, NameIdMapper *name_id_mapper,
                 uint64_t seq_num, utils::FileRetainer *file_retainer)
    : items_(items),
      name_id_mapper_(name_id_mapper),
      path_(wal_directory / MakeWalName()),
      from_timestamp_(0),
      to_timestamp_(0),
      count_(0),
      seq_num_(seq_num),
      file_retainer_(file_retainer) {
  // Ensure that the storage directory exists.
  utils::EnsureDirOrDie(wal_directory);

  // Initialize the WAL file.
  wal_.Initialize(path_, kWalMagic, kVersion);

  // Write placeholder offsets.
  uint64_t offset_offsets = 0;
  uint64_t offset_metadata = 0;
  uint64_t offset_deltas = 0;
  wal_.WriteMarker(Marker::SECTION_OFFSETS);
  offset_offsets = wal_.GetPosition();
  wal_.WriteUint(offset_metadata);
  wal_.WriteUint(offset_deltas);

  // Write metadata.
  offset_metadata = wal_.GetPosition();
  wal_.WriteMarker(Marker::SECTION_METADATA);
  wal_.WriteString(uuid);
  wal_.WriteString(epoch_id);
  wal_.WriteUint(seq_num);

  // Write final offsets.
  offset_deltas = wal_.GetPosition();
  wal_.SetPosition(offset_offsets);
  wal_.WriteUint(offset_metadata);
  wal_.WriteUint(offset_deltas);
  wal_.SetPosition(offset_deltas);

  // Sync the initial data.
  wal_.Sync();
}

WalFile::WalFile(std::filesystem::path current_wal_path, SalientConfig::Items items, NameIdMapper *name_id_mapper,
                 uint64_t seq_num, uint64_t from_timestamp, uint64_t to_timestamp, uint64_t count,
                 utils::FileRetainer *file_retainer)
    : items_(items),
      name_id_mapper_(name_id_mapper),
      path_(std::move(current_wal_path)),
      from_timestamp_(from_timestamp),
      to_timestamp_(to_timestamp),
      count_(count),
      seq_num_(seq_num),
      file_retainer_(file_retainer) {
  wal_.OpenExisting(path_);
}

void WalFile::FinalizeWal() {
  if (count_ != 0) {
    wal_.Finalize();
    // Rename file.
    std::filesystem::path new_path(path_);
    new_path.replace_filename(RemakeWalName(path_.filename(), from_timestamp_, to_timestamp_));

    utils::CopyFile(path_, new_path);
    wal_.Close();
    file_retainer_->DeleteFile(path_);
    path_ = std::move(new_path);
  }
}

void WalFile::DeleteWal() {
  wal_.Close();
  file_retainer_->DeleteFile(path_);
}

WalFile::~WalFile() {
  if (count_ == 0) {
    // Remove empty WAL file.
    utils::DeleteFile(path_);
  }
}

void WalFile::AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t timestamp) {
  EncodeDelta(&wal_, name_id_mapper_, items_, delta, vertex, timestamp);
  UpdateStats(timestamp);
}

void WalFile::AppendDelta(const Delta &delta, const Edge &edge, uint64_t timestamp) {
  EncodeDelta(&wal_, name_id_mapper_, delta, edge, timestamp);
  UpdateStats(timestamp);
}

void WalFile::AppendTransactionEnd(uint64_t timestamp) {
  EncodeTransactionEnd(&wal_, timestamp);
  UpdateStats(timestamp);
}

void WalFile::Sync() { wal_.Sync(); }

uint64_t WalFile::GetSize() { return wal_.GetSize(); }

uint64_t WalFile::SequenceNumber() const { return seq_num_; }

void WalFile::UpdateStats(uint64_t timestamp) {
  if (count_ == 0) from_timestamp_ = timestamp;
  to_timestamp_ = timestamp;
  count_ += 1;
}

void WalFile::DisableFlushing() { wal_.DisableFlushing(); }

void WalFile::EnableFlushing() { wal_.EnableFlushing(); }

void WalFile::TryFlushing() { wal_.TryFlushing(); }

std::pair<const uint8_t *, size_t> WalFile::CurrentFileBuffer() const { return wal_.CurrentFileBuffer(); }

void EncodeEnumAlterAdd(BaseEncoder &encoder, EnumStore const &enum_store, Enum enum_val) {
  auto etype_str = enum_store.ToTypeString(enum_val.type_id());
  DMG_ASSERT(etype_str.HasValue());
  encoder.WriteString(*etype_str);
  auto value_str = enum_store.ToValueString(enum_val.type_id(), enum_val.value_id());
  DMG_ASSERT(value_str.HasValue());
  encoder.WriteString(*value_str);
}

void EncodeEnumAlterUpdate(BaseEncoder &encoder, EnumStore const &enum_store, Enum enum_val,
                           std::string enum_value_old) {
  auto etype_str = enum_store.ToTypeString(enum_val.type_id());
  DMG_ASSERT(etype_str.HasValue());
  encoder.WriteString(*etype_str);
  encoder.WriteString(enum_value_old);
  auto value_str = enum_store.ToValueString(enum_val.type_id(), enum_val.value_id());
  DMG_ASSERT(value_str.HasValue());
  encoder.WriteString(*value_str);
}

void EncodeEnumCreate(BaseEncoder &encoder, EnumStore const &enum_store, EnumTypeId etype) {
  auto etype_str = enum_store.ToTypeString(etype);
  DMG_ASSERT(etype_str.HasValue());
  encoder.WriteString(*etype_str);
  auto const *values = enum_store.ToValuesStrings(etype);
  DMG_ASSERT(values);
  encoder.WriteUint(values->size());
  for (auto const &value : *values) {
    encoder.WriteString(value);
  }
}

void EncodeLabel(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
}

void EncodeLabelProperty(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, PropertyId prop) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(prop.AsUint()));
}

void EncodeLabelPropertyStats(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, PropertyId prop,
                              LabelPropertyIndexStats const &stats) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(prop.AsUint()));
  encoder.WriteString(ToJson(stats));
}

void EncodeLabelStats(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, LabelIndexStats stats) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteString(ToJson(stats));
}

void EncodeEdgeTypeIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, EdgeTypeId edge_type) {
  encoder.WriteString(name_id_mapper.IdToName(edge_type.AsUint()));
}

void EncodeEdgeTypePropertyIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, EdgeTypeId edge_type,
                                 PropertyId prop) {
  encoder.WriteString(name_id_mapper.IdToName(edge_type.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(prop.AsUint()));
}

void EncodeLabelProperties(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                           std::set<PropertyId> const &properties) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteUint(properties.size());
  for (const auto &property : properties) {
    encoder.WriteString(name_id_mapper.IdToName(property.AsUint()));
  }
}

void EncodeTypeConstraint(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, PropertyId property,
                          TypeConstraintKind type) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(property.AsUint()));
  encoder.WriteUint(static_cast<uint64_t>(type));
}

void EncodeTextIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, std::string_view text_index_name,
                     LabelId label) {
  encoder.WriteString(text_index_name);
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
}

void EncodeOperationPreamble(BaseEncoder &encoder, StorageMetadataOperation Op, uint64_t timestamp) {
  encoder.WriteMarker(Marker::SECTION_DELTA);
  encoder.WriteUint(timestamp);
  encoder.WriteMarker(OperationToMarker(Op));
}

}  // namespace memgraph::storage::durability
