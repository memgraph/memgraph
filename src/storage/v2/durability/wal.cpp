#include "storage/v2/durability/wal.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/vertex.hpp"

namespace storage::durability {

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

Marker OperationToMarker(StorageGlobalOperation operation) {
  switch (operation) {
    case StorageGlobalOperation::LABEL_INDEX_CREATE:
      return Marker::DELTA_LABEL_INDEX_CREATE;
    case StorageGlobalOperation::LABEL_INDEX_DROP:
      return Marker::DELTA_LABEL_INDEX_DROP;
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE:
      return Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE;
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP:
      return Marker::DELTA_LABEL_PROPERTY_INDEX_DROP;
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE:
      return Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE;
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP:
      return Marker::DELTA_EXISTENCE_CONSTRAINT_DROP;
    case StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE:
      return Marker::DELTA_UNIQUE_CONSTRAINT_CREATE;
    case StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP:
      return Marker::DELTA_UNIQUE_CONSTRAINT_DROP;
  }
}

Marker VertexActionToMarker(Delta::Action action) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  switch (action) {
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
      return Marker::DELTA_EDGE_DELETE;
    case Delta::Action::ADD_OUT_EDGE:
      return Marker::DELTA_EDGE_DELETE;
    case Delta::Action::REMOVE_IN_EDGE:
      return Marker::DELTA_EDGE_CREATE;
    case Delta::Action::REMOVE_OUT_EDGE:
      return Marker::DELTA_EDGE_CREATE;
  }
}

// This function convertes a Marker to a WalDeltaData::Type. It checks for the
// validity of the marker and throws if an invalid marker is specified.
// @throw RecoveryFailure
WalDeltaData::Type MarkerToWalDeltaDataType(Marker marker) {
  switch (marker) {
    case Marker::DELTA_VERTEX_CREATE:
      return WalDeltaData::Type::VERTEX_CREATE;
    case Marker::DELTA_VERTEX_DELETE:
      return WalDeltaData::Type::VERTEX_DELETE;
    case Marker::DELTA_VERTEX_ADD_LABEL:
      return WalDeltaData::Type::VERTEX_ADD_LABEL;
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
      return WalDeltaData::Type::VERTEX_REMOVE_LABEL;
    case Marker::DELTA_EDGE_CREATE:
      return WalDeltaData::Type::EDGE_CREATE;
    case Marker::DELTA_EDGE_DELETE:
      return WalDeltaData::Type::EDGE_DELETE;
    case Marker::DELTA_VERTEX_SET_PROPERTY:
      return WalDeltaData::Type::VERTEX_SET_PROPERTY;
    case Marker::DELTA_EDGE_SET_PROPERTY:
      return WalDeltaData::Type::EDGE_SET_PROPERTY;
    case Marker::DELTA_TRANSACTION_END:
      return WalDeltaData::Type::TRANSACTION_END;
    case Marker::DELTA_LABEL_INDEX_CREATE:
      return WalDeltaData::Type::LABEL_INDEX_CREATE;
    case Marker::DELTA_LABEL_INDEX_DROP:
      return WalDeltaData::Type::LABEL_INDEX_DROP;
    case Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
      return WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE;
    case Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
      return WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP;
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
      return WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE;
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
      return WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP;
    case Marker::DELTA_UNIQUE_CONSTRAINT_CREATE:
      return WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE;
    case Marker::DELTA_UNIQUE_CONSTRAINT_DROP:
      return WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP;

    case Marker::TYPE_NULL:
    case Marker::TYPE_BOOL:
    case Marker::TYPE_INT:
    case Marker::TYPE_DOUBLE:
    case Marker::TYPE_STRING:
    case Marker::TYPE_LIST:
    case Marker::TYPE_MAP:
    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_OFFSETS:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      throw RecoveryFailure("Invalid WAL data!");
  }
}

bool IsWalDeltaDataTypeTransactionEnd(WalDeltaData::Type type) {
  switch (type) {
    // These delta actions are all found inside transactions so they don't
    // indicate a transaction end.
    case WalDeltaData::Type::VERTEX_CREATE:
    case WalDeltaData::Type::VERTEX_DELETE:
    case WalDeltaData::Type::VERTEX_ADD_LABEL:
    case WalDeltaData::Type::VERTEX_REMOVE_LABEL:
    case WalDeltaData::Type::EDGE_CREATE:
    case WalDeltaData::Type::EDGE_DELETE:
    case WalDeltaData::Type::VERTEX_SET_PROPERTY:
    case WalDeltaData::Type::EDGE_SET_PROPERTY:
      return false;

    // This delta explicitly indicates that a transaction is done.
    case WalDeltaData::Type::TRANSACTION_END:
      return true;

    // These operations aren't transactional and they are encoded only using
    // a single delta, so they each individually mark the end of their
    // 'transaction'.
    case WalDeltaData::Type::LABEL_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_INDEX_DROP:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP:
      return true;
  }
}

// Function used to either read or skip the current WAL delta data. The WAL
// delta header must be read before calling this function. If the delta data is
// read then the data returned is valid, if the delta data is skipped then the
// returned data is not guaranteed to be set (it could be empty) and shouldn't
// be used.
// @throw RecoveryFailure
template <bool read_data>
WalDeltaData ReadSkipWalDeltaData(Decoder *wal) {
  WalDeltaData delta;

  auto action = wal->ReadMarker();
  if (!action) throw RecoveryFailure("Invalid WAL data!");
  delta.type = MarkerToWalDeltaDataType(*action);

  switch (delta.type) {
    case WalDeltaData::Type::VERTEX_CREATE:
    case WalDeltaData::Type::VERTEX_DELETE: {
      auto gid = wal->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_create_delete.gid = Gid::FromUint(*gid);
      break;
    }
    case WalDeltaData::Type::VERTEX_ADD_LABEL:
    case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
      auto gid = wal->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_add_remove_label.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto label = wal->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_add_remove_label.label = std::move(*label);
      } else {
        if (!wal->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::VERTEX_SET_PROPERTY:
    case WalDeltaData::Type::EDGE_SET_PROPERTY: {
      auto gid = wal->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.vertex_edge_set_property.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto property = wal->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_edge_set_property.property = std::move(*property);
        auto value = wal->ReadPropertyValue();
        if (!value) throw RecoveryFailure("Invalid WAL data!");
        delta.vertex_edge_set_property.value = std::move(*value);
      } else {
        if (!wal->SkipString() || !wal->SkipPropertyValue())
          throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::EDGE_CREATE:
    case WalDeltaData::Type::EDGE_DELETE: {
      auto gid = wal->ReadUint();
      if (!gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.gid = Gid::FromUint(*gid);
      if constexpr (read_data) {
        auto edge_type = wal->ReadString();
        if (!edge_type) throw RecoveryFailure("Invalid WAL data!");
        delta.edge_create_delete.edge_type = std::move(*edge_type);
      } else {
        if (!wal->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      auto from_gid = wal->ReadUint();
      if (!from_gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.from_vertex = Gid::FromUint(*from_gid);
      auto to_gid = wal->ReadUint();
      if (!to_gid) throw RecoveryFailure("Invalid WAL data!");
      delta.edge_create_delete.to_vertex = Gid::FromUint(*to_gid);
      break;
    }
    case WalDeltaData::Type::TRANSACTION_END:
      break;
    case WalDeltaData::Type::LABEL_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_INDEX_DROP: {
      if constexpr (read_data) {
        auto label = wal->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label.label = std::move(*label);
      } else {
        if (!wal->SkipString()) throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
      if constexpr (read_data) {
        auto label = wal->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property.label = std::move(*label);
        auto property = wal->ReadString();
        if (!property) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_property.property = std::move(*property);
      } else {
        if (!wal->SkipString() || !wal->SkipString())
          throw RecoveryFailure("Invalid WAL data!");
      }
      break;
    }
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
      if constexpr (read_data) {
        auto label = wal->ReadString();
        if (!label) throw RecoveryFailure("Invalid WAL data!");
        delta.operation_label_properties.label = std::move(*label);
        auto properties_count = wal->ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid WAL data!");
        for (uint64_t i = 0; i < *properties_count; ++i) {
          auto property = wal->ReadString();
          if (!property) throw RecoveryFailure("Invalid WAL data!");
          delta.operation_label_properties.properties.emplace(
              std::move(*property));
        }
      } else {
        if (!wal->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        auto properties_count = wal->ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid WAL data!");
        for (uint64_t i = 0; i < *properties_count; ++i) {
          if (!wal->SkipString()) throw RecoveryFailure("Invalid WAL data!");
        }
      }
    }
  }

  return delta;
}

}  // namespace

// Function used to read information about the WAL file.
WalInfo ReadWalInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (!IsVersionSupported(*version))
    throw RecoveryFailure("Invalid WAL version!");

  // Prepare return value.
  WalInfo info;

  // Read offsets.
  {
    auto marker = wal.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS)
      throw RecoveryFailure("Invalid WAL data!");

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
    if (!marker || *marker != Marker::SECTION_METADATA)
      throw RecoveryFailure("Invalid WAL data!");

    auto maybe_uuid = wal.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Invalid WAL data!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_seq_num = wal.ReadUint();
    if (!maybe_seq_num) throw RecoveryFailure("Invalid WAL data!");
    info.seq_num = *maybe_seq_num;
  }

  // Read deltas.
  info.num_deltas = 0;
  auto validate_delta = [&wal]() -> std::optional<std::pair<uint64_t, bool>> {
    try {
      auto timestamp = ReadWalDeltaHeader(&wal);
      auto type = SkipWalDeltaData(&wal);
      return {{timestamp, IsWalDeltaDataTypeTransactionEnd(type)}};
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
      if (timestamp < info.from_timestamp || timestamp < info.to_timestamp)
        break;
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
             a.vertex_edge_set_property.property ==
                 b.vertex_edge_set_property.property &&
             a.vertex_edge_set_property.value ==
                 b.vertex_edge_set_property.value;

    case WalDeltaData::Type::EDGE_CREATE:
    case WalDeltaData::Type::EDGE_DELETE:
      return a.edge_create_delete.gid == b.edge_create_delete.gid &&
             a.edge_create_delete.edge_type == b.edge_create_delete.edge_type &&
             a.edge_create_delete.from_vertex ==
                 b.edge_create_delete.from_vertex &&
             a.edge_create_delete.to_vertex == b.edge_create_delete.to_vertex;

    case WalDeltaData::Type::TRANSACTION_END:
      return true;

    case WalDeltaData::Type::LABEL_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_INDEX_DROP:
      return a.operation_label.label == b.operation_label.label;

    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP:
      return a.operation_label_property.label ==
                 b.operation_label_property.label &&
             a.operation_label_property.property ==
                 b.operation_label_property.property;
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP:
      return a.operation_label_properties.label ==
                 b.operation_label_properties.label &&
             a.operation_label_properties.properties ==
                 b.operation_label_properties.properties;
  }
}
bool operator!=(const WalDeltaData &a, const WalDeltaData &b) {
  return !(a == b);
}

// Function used to read the WAL delta header. The function returns the delta
// timestamp.
uint64_t ReadWalDeltaHeader(Decoder *wal) {
  auto marker = wal->ReadMarker();
  if (!marker || *marker != Marker::SECTION_DELTA)
    throw RecoveryFailure("Invalid WAL data!");

  auto timestamp = wal->ReadUint();
  if (!timestamp) throw RecoveryFailure("Invalid WAL data!");
  return *timestamp;
}

// Function used to read the current WAL delta data. The WAL delta header must
// be read before calling this function.
WalDeltaData ReadWalDeltaData(Decoder *wal) {
  return ReadSkipWalDeltaData<true>(wal);
}

// Function used to skip the current WAL delta data. The WAL delta header must
// be read before calling this function.
WalDeltaData::Type SkipWalDeltaData(Decoder *wal) {
  auto delta = ReadSkipWalDeltaData<false>(wal);
  return delta.type;
}

WalFile::WalFile(const std::filesystem::path &wal_directory,
                 const std::string &uuid, Config::Items items,
                 NameIdMapper *name_id_mapper, uint64_t seq_num)
    : items_(items),
      name_id_mapper_(name_id_mapper),
      path_(wal_directory / MakeWalName()),
      from_timestamp_(0),
      to_timestamp_(0),
      count_(0) {
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

WalFile::~WalFile() {
  if (count_ != 0) {
    // Finalize file.
    wal_.Finalize();

    // Rename file.
    std::filesystem::path new_path(path_);
    new_path.replace_filename(
        RemakeWalName(path_.filename(), from_timestamp_, to_timestamp_));
    // If the rename fails it isn't a crucial situation. The renaming is done
    // only to make the directory structure of the WAL files easier to read
    // manually.
    utils::RenamePath(path_, new_path);
  } else {
    // Remove empty WAL file.
    utils::DeleteFile(path_);
  }
}

void WalFile::AppendDelta(const Delta &delta, const Vertex &vertex,
                          uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  std::lock_guard<utils::SpinLock> guard(vertex.lock);
  switch (delta.action) {
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT: {
      wal_.WriteMarker(VertexActionToMarker(delta.action));
      wal_.WriteUint(vertex.gid.AsUint());
      break;
    }
    case Delta::Action::SET_PROPERTY: {
      wal_.WriteMarker(Marker::DELTA_VERTEX_SET_PROPERTY);
      wal_.WriteUint(vertex.gid.AsUint());
      wal_.WriteString(name_id_mapper_->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // vertex.
      // TODO (mferencevic): Mitigate the memory allocation introduced here
      // (with the `GetProperty` call). It is the only memory allocation in the
      // entire WAL file writing logic.
      wal_.WritePropertyValue(
          vertex.properties.GetProperty(delta.property.key));
      break;
    }
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL: {
      wal_.WriteMarker(VertexActionToMarker(delta.action));
      wal_.WriteUint(vertex.gid.AsUint());
      wal_.WriteString(name_id_mapper_->IdToName(delta.label.AsUint()));
      break;
    }
    case Delta::Action::ADD_OUT_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE: {
      wal_.WriteMarker(VertexActionToMarker(delta.action));
      if (items_.properties_on_edges) {
        wal_.WriteUint(delta.vertex_edge.edge.ptr->gid.AsUint());
      } else {
        wal_.WriteUint(delta.vertex_edge.edge.gid.AsUint());
      }
      wal_.WriteString(
          name_id_mapper_->IdToName(delta.vertex_edge.edge_type.AsUint()));
      wal_.WriteUint(vertex.gid.AsUint());
      wal_.WriteUint(delta.vertex_edge.vertex->gid.AsUint());
      break;
    }
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::REMOVE_IN_EDGE:
      // These actions are already encoded in the *_OUT_EDGE actions. This
      // function should never be called for this type of deltas.
      LOG(FATAL) << "Invalid delta action!";
  }
  UpdateStats(timestamp);
}

void WalFile::AppendDelta(const Delta &delta, const Edge &edge,
                          uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  std::lock_guard<utils::SpinLock> guard(edge.lock);
  switch (delta.action) {
    case Delta::Action::SET_PROPERTY: {
      wal_.WriteMarker(Marker::DELTA_EDGE_SET_PROPERTY);
      wal_.WriteUint(edge.gid.AsUint());
      wal_.WriteString(name_id_mapper_->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // edge.
      // TODO (mferencevic): Mitigate the memory allocation introduced here
      // (with the `GetProperty` call). It is the only memory allocation in the
      // entire WAL file writing logic.
      wal_.WritePropertyValue(edge.properties.GetProperty(delta.property.key));
      break;
    }
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT:
      // These actions are already encoded in vertex *_OUT_EDGE actions. Also,
      // these deltas don't contain any information about the from vertex, to
      // vertex or edge type so they are useless. This function should never
      // be called for this type of deltas.
      LOG(FATAL) << "Invalid delta action!";
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL:
    case Delta::Action::ADD_OUT_EDGE:
    case Delta::Action::REMOVE_OUT_EDGE:
    case Delta::Action::ADD_IN_EDGE:
    case Delta::Action::REMOVE_IN_EDGE:
      // These deltas shouldn't appear for edges.
      LOG(FATAL) << "Invalid database state!";
  }
  UpdateStats(timestamp);
}

void WalFile::AppendTransactionEnd(uint64_t timestamp) {
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  wal_.WriteMarker(Marker::DELTA_TRANSACTION_END);
  UpdateStats(timestamp);
}

void WalFile::AppendOperation(StorageGlobalOperation operation, LabelId label,
                              const std::set<PropertyId> &properties,
                              uint64_t timestamp) {
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  switch (operation) {
    case StorageGlobalOperation::LABEL_INDEX_CREATE:
    case StorageGlobalOperation::LABEL_INDEX_DROP: {
      CHECK(properties.empty()) << "Invalid function call!";
      wal_.WriteMarker(OperationToMarker(operation));
      wal_.WriteString(name_id_mapper_->IdToName(label.AsUint()));
      break;
    }
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE:
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP:
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE:
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP: {
      CHECK(properties.size() == 1) << "Invalid function call!";
      wal_.WriteMarker(OperationToMarker(operation));
      wal_.WriteString(name_id_mapper_->IdToName(label.AsUint()));
      wal_.WriteString(
          name_id_mapper_->IdToName((*properties.begin()).AsUint()));
      break;
    }
    case StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE:
    case StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP: {
      CHECK(!properties.empty()) << "Invalid function call!";
      wal_.WriteMarker(OperationToMarker(operation));
      wal_.WriteString(name_id_mapper_->IdToName(label.AsUint()));
      wal_.WriteUint(properties.size());
      for (const auto &property : properties) {
        wal_.WriteString(name_id_mapper_->IdToName(property.AsUint()));
      }
      break;
    }
  }
  UpdateStats(timestamp);
}

void WalFile::Sync() { wal_.Sync(); }

uint64_t WalFile::GetSize() { return wal_.GetPosition(); }

void WalFile::UpdateStats(uint64_t timestamp) {
  if (count_ == 0) from_timestamp_ = timestamp;
  to_timestamp_ = timestamp;
  count_ += 1;
}

}  // namespace storage::durability
