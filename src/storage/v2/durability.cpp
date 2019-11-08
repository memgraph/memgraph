#include "storage/v2/durability.hpp"

#include <algorithm>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/endian.hpp"
#include "utils/file.hpp"
#include "utils/timestamp.hpp"
#include "utils/uuid.hpp"

namespace storage {

//////////////////////////
// Encoder implementation.
//////////////////////////

namespace {
void WriteSize(Encoder *encoder, uint64_t size) {
  size = utils::HostToLittleEndian(size);
  encoder->Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size));
}
}  // namespace

void Encoder::Initialize(const std::filesystem::path &path,
                         const std::string_view &magic, uint64_t version) {
  file_.Open(path, utils::OutputFile::Mode::OVERWRITE_EXISTING);
  Write(reinterpret_cast<const uint8_t *>(magic.data()), magic.size());
  auto version_encoded = utils::HostToLittleEndian(version);
  Write(reinterpret_cast<const uint8_t *>(&version_encoded),
        sizeof(version_encoded));
}

void Encoder::Write(const uint8_t *data, uint64_t size) {
  file_.Write(data, size);
}

void Encoder::WriteMarker(Marker marker) {
  auto value = static_cast<uint8_t>(marker);
  Write(&value, sizeof(value));
}

void Encoder::WriteBool(bool value) {
  WriteMarker(Marker::TYPE_BOOL);
  if (value) {
    WriteMarker(Marker::VALUE_TRUE);
  } else {
    WriteMarker(Marker::VALUE_FALSE);
  }
}

void Encoder::WriteUint(uint64_t value) {
  value = utils::HostToLittleEndian(value);
  WriteMarker(Marker::TYPE_INT);
  Write(reinterpret_cast<const uint8_t *>(&value), sizeof(value));
}

void Encoder::WriteDouble(double value) {
  auto value_uint = utils::MemcpyCast<uint64_t>(value);
  value_uint = utils::HostToLittleEndian(value_uint);
  WriteMarker(Marker::TYPE_DOUBLE);
  Write(reinterpret_cast<const uint8_t *>(&value_uint), sizeof(value_uint));
}

void Encoder::WriteString(const std::string_view &value) {
  WriteMarker(Marker::TYPE_STRING);
  WriteSize(this, value.size());
  Write(reinterpret_cast<const uint8_t *>(value.data()), value.size());
}

void Encoder::WritePropertyValue(const PropertyValue &value) {
  WriteMarker(Marker::TYPE_PROPERTY_VALUE);
  switch (value.type()) {
    case PropertyValue::Type::Null: {
      WriteMarker(Marker::TYPE_NULL);
      break;
    }
    case PropertyValue::Type::Bool: {
      WriteBool(value.ValueBool());
      break;
    }
    case PropertyValue::Type::Int: {
      WriteUint(utils::MemcpyCast<uint64_t>(value.ValueInt()));
      break;
    }
    case PropertyValue::Type::Double: {
      WriteDouble(value.ValueDouble());
      break;
    }
    case PropertyValue::Type::String: {
      WriteString(value.ValueString());
      break;
    }
    case PropertyValue::Type::List: {
      const auto &list = value.ValueList();
      WriteMarker(Marker::TYPE_LIST);
      WriteSize(this, list.size());
      for (const auto &item : list) {
        WritePropertyValue(item);
      }
      break;
    }
    case PropertyValue::Type::Map: {
      const auto &map = value.ValueMap();
      WriteMarker(Marker::TYPE_MAP);
      WriteSize(this, map.size());
      for (const auto &item : map) {
        WriteString(item.first);
        WritePropertyValue(item.second);
      }
      break;
    }
  }
}

uint64_t Encoder::GetPosition() { return file_.GetPosition(); }

void Encoder::SetPosition(uint64_t position) {
  file_.SetPosition(utils::OutputFile::Position::SET, position);
}

void Encoder::Sync() { file_.Sync(); }

void Encoder::Finalize() {
  file_.Sync();
  file_.Close();
}

//////////////////////////
// Decoder implementation.
//////////////////////////

namespace {
std::optional<Marker> CastToMarker(uint8_t value) {
  for (auto marker : kMarkersAll) {
    if (static_cast<uint8_t>(marker) == value) {
      return marker;
    }
  }
  return std::nullopt;
}

std::optional<uint64_t> ReadSize(Decoder *decoder) {
  uint64_t size;
  if (!decoder->Read(reinterpret_cast<uint8_t *>(&size), sizeof(size)))
    return std::nullopt;
  size = utils::LittleEndianToHost(size);
  return size;
}
}  // namespace

std::optional<uint64_t> Decoder::Initialize(const std::filesystem::path &path,
                                            const std::string &magic) {
  if (!file_.Open(path)) return std::nullopt;
  std::string file_magic(magic.size(), '\0');
  if (!Read(reinterpret_cast<uint8_t *>(file_magic.data()), file_magic.size()))
    return std::nullopt;
  if (file_magic != magic) return std::nullopt;
  uint64_t version_encoded;
  if (!Read(reinterpret_cast<uint8_t *>(&version_encoded),
            sizeof(version_encoded)))
    return std::nullopt;
  return utils::LittleEndianToHost(version_encoded);
}

bool Decoder::Read(uint8_t *data, size_t size) {
  return file_.Read(data, size);
}

bool Decoder::Peek(uint8_t *data, size_t size) {
  return file_.Peek(data, size);
}

std::optional<Marker> Decoder::PeekMarker() {
  uint8_t value;
  if (!Peek(&value, sizeof(value))) return std::nullopt;
  auto marker = CastToMarker(value);
  if (!marker) return std::nullopt;
  return *marker;
}

std::optional<Marker> Decoder::ReadMarker() {
  uint8_t value;
  if (!Read(&value, sizeof(value))) return std::nullopt;
  auto marker = CastToMarker(value);
  if (!marker) return std::nullopt;
  return *marker;
}

std::optional<bool> Decoder::ReadBool() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_BOOL) return std::nullopt;
  auto value = ReadMarker();
  if (!value || (*value != Marker::VALUE_FALSE && *value != Marker::VALUE_TRUE))
    return std::nullopt;
  return *value == Marker::VALUE_TRUE;
}

std::optional<uint64_t> Decoder::ReadUint() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_INT) return std::nullopt;
  uint64_t value;
  if (!Read(reinterpret_cast<uint8_t *>(&value), sizeof(value)))
    return std::nullopt;
  value = utils::LittleEndianToHost(value);
  return value;
}

std::optional<double> Decoder::ReadDouble() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_DOUBLE) return std::nullopt;
  uint64_t value_int;
  if (!Read(reinterpret_cast<uint8_t *>(&value_int), sizeof(value_int)))
    return std::nullopt;
  value_int = utils::LittleEndianToHost(value_int);
  auto value = utils::MemcpyCast<double>(value_int);
  return value;
}

std::optional<std::string> Decoder::ReadString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return std::nullopt;
  auto size = ReadSize(this);
  if (!size) return std::nullopt;
  std::string value(*size, '\0');
  if (!Read(reinterpret_cast<uint8_t *>(value.data()), *size))
    return std::nullopt;
  return value;
}

std::optional<PropertyValue> Decoder::ReadPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE)
    return std::nullopt;

  auto marker = PeekMarker();
  if (!marker) return std::nullopt;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_NULL)
        return std::nullopt;
      return PropertyValue();
    }
    case Marker::TYPE_BOOL: {
      auto value = ReadBool();
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Marker::TYPE_INT: {
      auto value = ReadUint();
      if (!value) return std::nullopt;
      return PropertyValue(utils::MemcpyCast<int64_t>(*value));
    }
    case Marker::TYPE_DOUBLE: {
      auto value = ReadDouble();
      if (!value) return std::nullopt;
      return PropertyValue(*value);
    }
    case Marker::TYPE_STRING: {
      auto value = ReadString();
      if (!value) return std::nullopt;
      return PropertyValue(std::move(*value));
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST)
        return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      std::vector<PropertyValue> value;
      value.reserve(*size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto item = ReadPropertyValue();
        if (!item) return std::nullopt;
        value.emplace_back(std::move(*item));
      }
      return PropertyValue(std::move(value));
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP)
        return std::nullopt;
      auto size = ReadSize(this);
      if (!size) return std::nullopt;
      std::map<std::string, PropertyValue> value;
      for (uint64_t i = 0; i < *size; ++i) {
        auto key = ReadString();
        if (!key) return std::nullopt;
        auto item = ReadPropertyValue();
        if (!item) return std::nullopt;
        value.emplace(std::move(*key), std::move(*item));
      }
      return PropertyValue(std::move(value));
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_OFFSETS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return std::nullopt;
  }
}

bool Decoder::SkipString() {
  auto marker = ReadMarker();
  if (!marker || *marker != Marker::TYPE_STRING) return false;
  auto maybe_size = ReadSize(this);
  if (!maybe_size) return false;

  const uint64_t kBufferSize = 262144;
  uint8_t buffer[kBufferSize];
  uint64_t size = *maybe_size;
  while (size > 0) {
    uint64_t to_read = size < kBufferSize ? size : kBufferSize;
    if (!Read(reinterpret_cast<uint8_t *>(&buffer), to_read)) return false;
    size -= to_read;
  }

  return true;
}

bool Decoder::SkipPropertyValue() {
  auto pv_marker = ReadMarker();
  if (!pv_marker || *pv_marker != Marker::TYPE_PROPERTY_VALUE) return false;

  auto marker = PeekMarker();
  if (!marker) return false;
  switch (*marker) {
    case Marker::TYPE_NULL: {
      auto inner_marker = ReadMarker();
      return inner_marker && *inner_marker == Marker::TYPE_NULL;
    }
    case Marker::TYPE_BOOL: {
      return !!ReadBool();
    }
    case Marker::TYPE_INT: {
      return !!ReadUint();
    }
    case Marker::TYPE_DOUBLE: {
      return !!ReadDouble();
    }
    case Marker::TYPE_STRING: {
      return SkipString();
    }
    case Marker::TYPE_LIST: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_LIST) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipPropertyValue()) return false;
      }
      return true;
    }
    case Marker::TYPE_MAP: {
      auto inner_marker = ReadMarker();
      if (!inner_marker || *inner_marker != Marker::TYPE_MAP) return false;
      auto size = ReadSize(this);
      if (!size) return false;
      for (uint64_t i = 0; i < *size; ++i) {
        if (!SkipString()) return false;
        if (!SkipPropertyValue()) return false;
      }
      return true;
    }

    case Marker::TYPE_PROPERTY_VALUE:
    case Marker::SECTION_VERTEX:
    case Marker::SECTION_EDGE:
    case Marker::SECTION_MAPPER:
    case Marker::SECTION_METADATA:
    case Marker::SECTION_INDICES:
    case Marker::SECTION_CONSTRAINTS:
    case Marker::SECTION_DELTA:
    case Marker::SECTION_OFFSETS:
    case Marker::DELTA_VERTEX_CREATE:
    case Marker::DELTA_VERTEX_DELETE:
    case Marker::DELTA_VERTEX_ADD_LABEL:
    case Marker::DELTA_VERTEX_REMOVE_LABEL:
    case Marker::DELTA_VERTEX_SET_PROPERTY:
    case Marker::DELTA_EDGE_CREATE:
    case Marker::DELTA_EDGE_DELETE:
    case Marker::DELTA_EDGE_SET_PROPERTY:
    case Marker::DELTA_TRANSACTION_END:
    case Marker::DELTA_LABEL_INDEX_CREATE:
    case Marker::DELTA_LABEL_INDEX_DROP:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE:
    case Marker::DELTA_LABEL_PROPERTY_INDEX_DROP:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case Marker::DELTA_EXISTENCE_CONSTRAINT_DROP:
    case Marker::VALUE_FALSE:
    case Marker::VALUE_TRUE:
      return false;
  }
}

std::optional<uint64_t> Decoder::GetSize() { return file_.GetSize(); }

std::optional<uint64_t> Decoder::GetPosition() { return file_.GetPosition(); }

bool Decoder::SetPosition(uint64_t position) {
  return !!file_.SetPosition(utils::InputFile::Position::SET, position);
}

/////////////////////////////
// Durability implementation.
/////////////////////////////

namespace {
// The current version of snapshot and WAL encoding / decoding.
// IMPORTANT: Please bump this version for every snapshot and/or WAL format
// change!!!
const uint64_t kVersion{12};

// Snapshot format:
//
// 1) Magic string (non-encoded)
//
// 2) Snapshot version (non-encoded, little-endian)
//
// 3) Section offsets:
//     * offset to the first edge in the snapshot (`0` if properties on edges
//       are disabled)
//     * offset to the first vertex in the snapshot
//     * offset to the indices section
//     * offset to the constraints section
//     * offset to the mapper section
//     * offset to the metadata section
//
// 4) Encoded edges (if properties on edges are enabled); each edge is written
//    in the following format:
//     * gid
//     * properties
//
// 5) Encoded vertices; each vertex is written in the following format:
//     * gid
//     * labels
//     * properties
//     * in edges
//         * edge gid
//         * from vertex gid
//         * edge type
//     * out edges
//         * edge gid
//         * to vertex gid
//         * edge type
//
// 6) Indices
//     * label indices
//         * label
//     * label+property indices
//         * label
//         * property
//
// 7) Constraints
//     * existence constraints
//         * label
//         * property
//
// 8) Name to ID mapper data
//     * id to name mappings
//         * id
//         * name
//
// 9) Metadata
//     * storage UUID
//     * snapshot transaction start timestamp (required when recovering
//       from snapshot combined with WAL to determine what deltas need to be
//       applied)
//     * number of edges
//     * number of vertices

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

// This is the prefix used for Snapshot and WAL filenames. It is a timestamp
// format that equals to: YYYYmmddHHMMSSffffff
const std::string kTimestampFormat =
    "{:04d}{:02d}{:02d}{:02d}{:02d}{:02d}{:06d}";

// Generates the name for a snapshot in a well-defined sortable format with the
// start timestamp appended to the file name.
std::string MakeSnapshotName(uint64_t start_timestamp) {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_timestamp_" + std::to_string(start_timestamp);
}

// Generates the name for a WAL file in a well-defined sortable format.
std::string MakeWalName() {
  std::string date_str = utils::Timestamp::Now().ToString(kTimestampFormat);
  return date_str + "_current";
}

// Generates the name for a WAL file in a well-defined sortable format with the
// range of timestamps contained [from, to] appended to the name.
std::string RemakeWalName(const std::string &current_name,
                          uint64_t from_timestamp, uint64_t to_timestamp) {
  return current_name.substr(0, current_name.size() - 8) + "_from_" +
         std::to_string(from_timestamp) + "_to_" + std::to_string(to_timestamp);
}

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
  }

  return delta;
}

// Helper function used to insert indices/constraints into the recovered
// indices/constraints object.
// @throw RecoveryFailure
template <typename TObj>
void AddRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj,
                                 const char *error_message) {
  auto it = std::find(list->begin(), list->end(), obj);
  if (it == list->end()) {
    list->push_back(obj);
  } else {
    throw RecoveryFailure(error_message);
  }
}

template <typename TObj>
void RemoveRecoveredIndexConstraint(std::vector<TObj> *list, TObj obj,
                                    const char *error_message) {
  auto it = std::find(list->begin(), list->end(), obj);
  if (it != list->end()) {
    std::swap(*it, list->back());
    list->pop_back();
  } else {
    throw RecoveryFailure(error_message);
  }
}
}  // namespace

// Function used to read information about the snapshot file.
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (*version != kVersion) throw RecoveryFailure("Invalid snapshot version!");

  // Prepare return value.
  SnapshotInfo info;

  // Read offsets.
  {
    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS)
      throw RecoveryFailure("Invalid snapshot data!");

    auto snapshot_size = snapshot.GetSize();
    if (!snapshot_size)
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto read_offset = [&snapshot, snapshot_size] {
      auto maybe_offset = snapshot.ReadUint();
      if (!maybe_offset) throw RecoveryFailure("Invalid snapshot format!");
      auto offset = *maybe_offset;
      if (offset > *snapshot_size)
        throw RecoveryFailure("Invalid snapshot format!");
      return offset;
    };

    info.offset_edges = read_offset();
    info.offset_vertices = read_offset();
    info.offset_indices = read_offset();
    info.offset_constraints = read_offset();
    info.offset_mapper = read_offset();
    info.offset_metadata = read_offset();
  }

  // Read metadata.
  {
    if (!snapshot.SetPosition(info.offset_metadata))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA)
      throw RecoveryFailure("Invalid snapshot data!");

    auto maybe_uuid = snapshot.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Invalid snapshot data!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_timestamp = snapshot.ReadUint();
    if (!maybe_timestamp) throw RecoveryFailure("Invalid snapshot data!");
    info.start_timestamp = *maybe_timestamp;

    auto maybe_edges = snapshot.ReadUint();
    if (!maybe_edges) throw RecoveryFailure("Invalid snapshot data!");
    info.edges_count = *maybe_edges;

    auto maybe_vertices = snapshot.ReadUint();
    if (!maybe_vertices) throw RecoveryFailure("Invalid snapshot data!");
    info.vertices_count = *maybe_vertices;
  }

  return info;
}

// Function used to read information about the WAL file.
WalInfo ReadWalInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (*version != kVersion) throw RecoveryFailure("Invalid WAL version!");

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

// Function used to either read the current WAL delta data. The WAL delta header
// must be read before calling this function.
WalDeltaData ReadWalDeltaData(Decoder *wal) {
  return ReadSkipWalDeltaData<true>(wal);
}

// Function used to either skip the current WAL delta data. The WAL delta header
// must be read before calling this function.
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

void WalFile::AppendDelta(const Delta &delta, Vertex *vertex,
                          uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  std::lock_guard<utils::SpinLock> guard(vertex->lock);
  switch (delta.action) {
    case Delta::Action::DELETE_OBJECT:
    case Delta::Action::RECREATE_OBJECT: {
      wal_.WriteMarker(VertexActionToMarker(delta.action));
      wal_.WriteUint(vertex->gid.AsUint());
      break;
    }
    case Delta::Action::SET_PROPERTY: {
      wal_.WriteMarker(Marker::DELTA_VERTEX_SET_PROPERTY);
      wal_.WriteUint(vertex->gid.AsUint());
      wal_.WriteString(name_id_mapper_->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // vertex.
      auto it = vertex->properties.find(delta.property.key);
      if (it != vertex->properties.end()) {
        wal_.WritePropertyValue(it->second);
      } else {
        wal_.WritePropertyValue(PropertyValue());
      }
      break;
    }
    case Delta::Action::ADD_LABEL:
    case Delta::Action::REMOVE_LABEL: {
      wal_.WriteMarker(VertexActionToMarker(delta.action));
      wal_.WriteUint(vertex->gid.AsUint());
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
      wal_.WriteUint(vertex->gid.AsUint());
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

void WalFile::AppendDelta(const Delta &delta, Edge *edge, uint64_t timestamp) {
  // When converting a Delta to a WAL delta the logic is inverted. That is
  // because the Delta's represent undo actions and we want to store redo
  // actions.
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  std::lock_guard<utils::SpinLock> guard(edge->lock);
  switch (delta.action) {
    case Delta::Action::SET_PROPERTY: {
      wal_.WriteMarker(Marker::DELTA_EDGE_SET_PROPERTY);
      wal_.WriteUint(edge->gid.AsUint());
      wal_.WriteString(name_id_mapper_->IdToName(delta.property.key.AsUint()));
      // The property value is the value that is currently stored in the
      // edge.
      auto it = edge->properties.find(delta.property.key);
      if (it != edge->properties.end()) {
        wal_.WritePropertyValue(it->second);
      } else {
        wal_.WritePropertyValue(PropertyValue());
      }
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
                              std::optional<PropertyId> property,
                              uint64_t timestamp) {
  wal_.WriteMarker(Marker::SECTION_DELTA);
  wal_.WriteUint(timestamp);
  switch (operation) {
    case StorageGlobalOperation::LABEL_INDEX_CREATE:
    case StorageGlobalOperation::LABEL_INDEX_DROP: {
      wal_.WriteMarker(OperationToMarker(operation));
      wal_.WriteString(name_id_mapper_->IdToName(label.AsUint()));
      break;
    }
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE:
    case StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP:
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE:
    case StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP: {
      CHECK(property) << "Invalid function call!";
      wal_.WriteMarker(OperationToMarker(operation));
      wal_.WriteString(name_id_mapper_->IdToName(label.AsUint()));
      wal_.WriteString(name_id_mapper_->IdToName(property->AsUint()));
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

Durability::Durability(Config::Durability config,
                       utils::SkipList<Vertex> *vertices,
                       utils::SkipList<Edge> *edges,
                       NameIdMapper *name_id_mapper, Indices *indices,
                       Constraints *constraints, Config::Items items)
    : config_(config),
      vertices_(vertices),
      edges_(edges),
      name_id_mapper_(name_id_mapper),
      indices_(indices),
      constraints_(constraints),
      items_(items),
      snapshot_directory_(config_.storage_directory / kSnapshotDirectory),
      wal_directory_(config_.storage_directory / kWalDirectory),
      uuid_(utils::GenerateUUID()) {}

std::optional<Durability::RecoveryInfo> Durability::Initialize(
    std::function<void(std::function<void(Transaction *)>)>
        execute_with_transaction) {
  execute_with_transaction_ = execute_with_transaction;
  std::optional<Durability::RecoveryInfo> ret;
  if (config_.recover_on_startup) {
    ret = RecoverData();
  } else if (config_.snapshot_wal_mode !=
                 Config::Durability::SnapshotWalMode::DISABLED ||
             config_.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root = config_.storage_directory / kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(snapshot_directory_, kSnapshotDirectory, "snapshot"),
          std::make_tuple(wal_directory_, kWalDirectory, "WAL")}) {
      if (!utils::DirExists(path)) continue;
      auto backup_curr = backup_root / dirname;
      std::error_code error_code;
      for (const auto &item :
           std::filesystem::directory_iterator(path, error_code)) {
        utils::EnsureDirOrDie(backup_root);
        utils::EnsureDirOrDie(backup_curr);
        std::error_code item_error_code;
        std::filesystem::rename(
            item.path(), backup_curr / item.path().filename(), item_error_code);
        CHECK(!item_error_code)
            << "Couldn't move " << what << " file " << item.path()
            << " because of: " << item_error_code.message();
        files_moved = true;
      }
      CHECK(!error_code) << "Couldn't backup " << what
                         << " files bacause of: " << error_code.message();
    }
    LOG_IF(WARNING, files_moved)
        << "Since Memgraph was not supposed to recover on startup and "
           "durability is enabled, your current durability files will likely "
           "be overriden. To prevent important data loss, Memgraph has stored "
           "those files into a .backup directory inside the storage directory.";
  }
  if (config_.snapshot_wal_mode !=
          Config::Durability::SnapshotWalMode::DISABLED ||
      config_.snapshot_on_exit) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
  }
  if (config_.snapshot_wal_mode ==
      Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL) {
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);
  }
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Run("Snapshot", config_.snapshot_interval, [this] {
      execute_with_transaction_(
          [this](Transaction *transaction) { CreateSnapshot(transaction); });
    });
  }
  return ret;
}

void Durability::Finalize() {
  wal_file_ = std::nullopt;
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::DISABLED) {
    snapshot_runner_.Stop();
  }
  if (config_.snapshot_on_exit) {
    execute_with_transaction_(
        [this](Transaction *transaction) { CreateSnapshot(transaction); });
  }
}

void Durability::AppendToWal(const Transaction &transaction,
                             uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp =
      transaction.commit_timestamp->load(std::memory_order_acquire);
  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, auto *parent,
                                   auto filter) {
    while (true) {
      auto older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr ||
          older->timestamp->load(std::memory_order_acquire) !=
              current_commit_timestamp)
        break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
      }
      auto prev = delta->prev.Get();
      if (prev.type != PreviousPtr::Type::DELTA) break;
      delta = prev.delta;
    }
  };

  // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
  // don't traverse them in that order. That is because for each delta we need
  // information about the vertex or edge they belong to and that information
  // isn't stored in the deltas themselves. In order to find out information
  // about the corresponding vertex or edge it is necessary to traverse the
  // delta chain for each delta until a vertex or edge is encountered. This
  // operation is very expensive as the chain grows.
  // Instead, we traverse the edges until we find a vertex or edge and traverse
  // their delta chains. This approach has a drawback because we lose the
  // correct order of the operations. Because of that, we need to traverse the
  // deltas several times and we have to manually ensure that the stored deltas
  // will be ordered correctly.

  // 1. Process all Vertex deltas and store all operations that create vertices
  // and modify vertex data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
          return true;

        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 2. Process all Vertex deltas and store all operations that create edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          return false;
      }
    });
  }
  // 3. Process all Edge deltas and store all operations that modify edge data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 4. Process all Vertex deltas and store all operations that delete edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 5. Process all Vertex deltas and store all operations that delete vertices.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.
  wal_file_->AppendTransactionEnd(final_commit_timestamp);

  FinalizeWalFile();
}

void Durability::AppendToWal(StorageGlobalOperation operation, LabelId label,
                             std::optional<PropertyId> property,
                             uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  wal_file_->AppendOperation(operation, label, property,
                             final_commit_timestamp);
  FinalizeWalFile();
}

void Durability::CreateSnapshot(Transaction *transaction) {
  // Ensure that the storage directory exists.
  utils::EnsureDirOrDie(snapshot_directory_);

  // Create snapshot file.
  auto path =
      snapshot_directory_ / MakeSnapshotName(transaction->start_timestamp);
  LOG(INFO) << "Starting snapshot creation to " << path;
  Encoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic, kVersion);

  // Write placeholder offsets.
  uint64_t offset_offsets = 0;
  uint64_t offset_edges = 0;
  uint64_t offset_vertices = 0;
  uint64_t offset_indices = 0;
  uint64_t offset_constraints = 0;
  uint64_t offset_mapper = 0;
  uint64_t offset_metadata = 0;
  {
    snapshot.WriteMarker(Marker::SECTION_OFFSETS);
    offset_offsets = snapshot.GetPosition();
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_metadata);
  }

  // Object counters.
  uint64_t edges_count = 0;
  uint64_t vertices_count = 0;

  // Mapper data.
  std::unordered_set<uint64_t> used_ids;
  auto write_mapping = [&snapshot, &used_ids](auto mapping) {
    used_ids.insert(mapping.AsUint());
    snapshot.WriteUint(mapping.AsUint());
  };

  // Store all edges.
  if (items_.properties_on_edges) {
    offset_edges = snapshot.GetPosition();
    auto acc = edges_->access();
    for (auto &edge : acc) {
      // The edge visibility check must be done here manually because we don't
      // allow direct access to the edges through the public API.
      bool is_visible = true;
      Delta *delta = nullptr;
      {
        std::lock_guard<utils::SpinLock> guard(edge.lock);
        is_visible = !edge.deleted;
        delta = edge.delta;
      }
      ApplyDeltasForRead(transaction, delta, View::OLD,
                         [&is_visible](const Delta &delta) {
                           switch (delta.action) {
                             case Delta::Action::ADD_LABEL:
                             case Delta::Action::REMOVE_LABEL:
                             case Delta::Action::SET_PROPERTY:
                             case Delta::Action::ADD_IN_EDGE:
                             case Delta::Action::ADD_OUT_EDGE:
                             case Delta::Action::REMOVE_IN_EDGE:
                             case Delta::Action::REMOVE_OUT_EDGE:
                               break;
                             case Delta::Action::RECREATE_OBJECT: {
                               is_visible = true;
                               break;
                             }
                             case Delta::Action::DELETE_OBJECT: {
                               is_visible = false;
                               break;
                             }
                           }
                         });
      if (!is_visible) continue;
      EdgeRef edge_ref(&edge);
      // Here we create an edge accessor that we will use to get the
      // properties of the edge. The accessor is created with an invalid
      // type and invalid from/to pointers because we don't know them here,
      // but that isn't an issue because we won't use that part of the API
      // here.
      auto ea = EdgeAccessor{edge_ref,    EdgeTypeId::FromUint(0UL),
                             nullptr,     nullptr,
                             transaction, indices_,
                             items_};

      // Get edge data.
      auto maybe_props = ea.Properties(View::OLD);
      CHECK(maybe_props.HasValue()) << "Invalid database state!";

      // Store the edge.
      {
        snapshot.WriteMarker(Marker::SECTION_EDGE);
        snapshot.WriteUint(edge.gid.AsUint());
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
      }

      ++edges_count;
    }
  }

  // Store all vertices.
  {
    offset_vertices = snapshot.GetPosition();
    auto acc = vertices_->access();
    for (auto &vertex : acc) {
      // The visibility check is implemented for vertices so we use it here.
      auto va = VertexAccessor::Create(&vertex, transaction, indices_, items_,
                                       View::OLD);
      if (!va) continue;

      // Get vertex data.
      // TODO (mferencevic): All of these functions could be written into a
      // single function so that we traverse the undo deltas only once.
      auto maybe_labels = va->Labels(View::OLD);
      CHECK(maybe_labels.HasValue()) << "Invalid database state!";
      auto maybe_props = va->Properties(View::OLD);
      CHECK(maybe_props.HasValue()) << "Invalid database state!";
      auto maybe_in_edges = va->InEdges({}, View::OLD);
      CHECK(maybe_in_edges.HasValue()) << "Invalid database state!";
      auto maybe_out_edges = va->OutEdges({}, View::OLD);
      CHECK(maybe_out_edges.HasValue()) << "Invalid database state!";

      // Store the vertex.
      {
        snapshot.WriteMarker(Marker::SECTION_VERTEX);
        snapshot.WriteUint(vertex.gid.AsUint());
        const auto &labels = maybe_labels.GetValue();
        snapshot.WriteUint(labels.size());
        for (const auto &item : labels) {
          write_mapping(item);
        }
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
        const auto &in_edges = maybe_in_edges.GetValue();
        snapshot.WriteUint(in_edges.size());
        for (const auto &item : in_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.FromVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
        const auto &out_edges = maybe_out_edges.GetValue();
        snapshot.WriteUint(out_edges.size());
        for (const auto &item : out_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.ToVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
      }

      ++vertices_count;
    }
  }

  // Write indices.
  {
    offset_indices = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_INDICES);

    // Write label indices.
    {
      auto label = indices_->label_index.ListIndices();
      snapshot.WriteUint(label.size());
      for (const auto &item : label) {
        write_mapping(item);
      }
    }

    // Write label+property indices.
    {
      auto label_property = indices_->label_property_index.ListIndices();
      snapshot.WriteUint(label_property.size());
      for (const auto &item : label_property) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }
  }

  // Write constraints.
  {
    offset_constraints = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_CONSTRAINTS);

    // Write existence constraints.
    {
      auto existence = ListExistenceConstraints(*constraints_);
      snapshot.WriteUint(existence.size());
      for (const auto &item : existence) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }
  }

  // Write mapper data.
  {
    offset_mapper = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_MAPPER);
    snapshot.WriteUint(used_ids.size());
    for (auto item : used_ids) {
      snapshot.WriteUint(item);
      snapshot.WriteString(name_id_mapper_->IdToName(item));
    }
  }

  // Write metadata.
  {
    offset_metadata = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_METADATA);
    snapshot.WriteString(uuid_);
    snapshot.WriteUint(transaction->start_timestamp);
    snapshot.WriteUint(edges_count);
    snapshot.WriteUint(vertices_count);
  }

  // Write true offsets.
  {
    snapshot.SetPosition(offset_offsets);
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_metadata);
  }

  // Finalize snapshot file.
  snapshot.Finalize();
  LOG(INFO) << "Snapshot creation successful!";

  // Ensure exactly `snapshot_retention_count` snapshots exist.
  std::vector<std::pair<uint64_t, std::filesystem::path>> old_snapshot_files;
  {
    std::error_code error_code;
    for (const auto &item :
         std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
      if (!item.is_regular_file()) continue;
      if (item.path() == path) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        if (info.uuid != uuid_) continue;
        old_snapshot_files.emplace_back(info.start_timestamp, item.path());
      } catch (const RecoveryFailure &e) {
        LOG(WARNING) << "Found a corrupt snapshot file " << item.path()
                     << " because of: " << e.what();
        continue;
      }
    }
    LOG_IF(ERROR, error_code)
        << "Couldn't ensure that exactly " << config_.snapshot_retention_count
        << " snapshots exist because an error occurred: "
        << error_code.message() << "!";
    std::sort(old_snapshot_files.begin(), old_snapshot_files.end());
    if (old_snapshot_files.size() > config_.snapshot_retention_count - 1) {
      auto num_to_erase =
          old_snapshot_files.size() - (config_.snapshot_retention_count - 1);
      for (size_t i = 0; i < num_to_erase; ++i) {
        const auto &[start_timestamp, snapshot_path] = old_snapshot_files[i];
        if (!utils::DeleteFile(snapshot_path)) {
          LOG(WARNING) << "Couldn't delete snapshot file " << snapshot_path
                       << "!";
        }
      }
      old_snapshot_files.erase(old_snapshot_files.begin(),
                               old_snapshot_files.begin() + num_to_erase);
    }
  }

  // Ensure that only the absolutely necessary WAL files exist.
  if (old_snapshot_files.size() == config_.snapshot_retention_count - 1 &&
      utils::DirExists(wal_directory_)) {
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>>
        wal_files;
    std::error_code error_code;
    for (const auto &item :
         std::filesystem::directory_iterator(wal_directory_, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadWalInfo(item.path());
        if (info.uuid != uuid_) continue;
        wal_files.emplace_back(info.seq_num, info.from_timestamp,
                               info.to_timestamp, item.path());
      } catch (const RecoveryFailure &e) {
        continue;
      }
    }
    LOG_IF(ERROR, error_code)
        << "Couldn't ensure that only the absolutely necessary WAL files exist "
           "because an error occurred: "
        << error_code.message() << "!";
    std::sort(wal_files.begin(), wal_files.end());
    uint64_t snapshot_start_timestamp = transaction->start_timestamp;
    if (!old_snapshot_files.empty()) {
      snapshot_start_timestamp = old_snapshot_files.front().first;
    }
    std::optional<uint64_t> pos = 0;
    for (uint64_t i = 0; i < wal_files.size(); ++i) {
      const auto &[seq_num, from_timestamp, to_timestamp, wal_path] =
          wal_files[i];
      if (to_timestamp <= snapshot_start_timestamp) {
        pos = i;
      } else {
        break;
      }
    }
    if (pos && *pos > 0) {
      // We need to leave at least one WAL file that contains deltas that were
      // created before the oldest snapshot. Because we always leave at least
      // one WAL file that contains deltas before the snapshot, this correctly
      // handles the edge case when that one file is the current WAL file that
      // is being appended to.
      for (uint64_t i = 0; i < *pos; ++i) {
        const auto &[seq_num, from_timestamp, to_timestamp, wal_path] =
            wal_files[i];
        if (!utils::DeleteFile(wal_path)) {
          LOG(WARNING) << "Couldn't delete WAL file " << wal_path << "!";
        }
      }
    }
  }
}

std::optional<Durability::RecoveryInfo> Durability::RecoverData() {
  if (!utils::DirExists(snapshot_directory_)) return std::nullopt;

  // Helper lambda used to recover all discovered indices and constraints. The
  // indices and constraints must be recovered after the data recovery is done
  // to ensure that the indices and constraints are consistent at the end of the
  // recovery process.
  auto recover_indices_and_constraints = [this](
                                             const auto &indices_constraints) {
    // Recover label indices.
    for (const auto &item : indices_constraints.indices.label) {
      if (!indices_->label_index.CreateIndex(item, vertices_->access()))
        throw RecoveryFailure("The label index must be created here!");
    }

    // Recover label+property indices.
    for (const auto &item : indices_constraints.indices.label_property) {
      if (!indices_->label_property_index.CreateIndex(item.first, item.second,
                                                      vertices_->access()))
        throw RecoveryFailure("The label+property index must be created here!");
    }

    // Recover existence constraints.
    for (const auto &item : indices_constraints.constraints.existence) {
      auto ret = CreateExistenceConstraint(constraints_, item.first,
                                           item.second, vertices_->access());
      if (ret.HasError() || !ret.GetValue())
        throw RecoveryFailure("The existence constraint must be created here!");
    }
  };

  // Array of all discovered snapshots, ordered by name.
  std::vector<std::pair<std::filesystem::path, std::string>> snapshot_files;
  std::error_code error_code;
  for (const auto &item :
       std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadSnapshotInfo(item.path());
      snapshot_files.emplace_back(item.path(), info.uuid);
    } catch (const RecoveryFailure &) {
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;
  std::optional<uint64_t> snapshot_timestamp;
  if (!snapshot_files.empty()) {
    std::sort(snapshot_files.begin(), snapshot_files.end());
    // UUID used for durability is the UUID of the last snapshot file.
    uuid_ = snapshot_files.back().second;
    std::optional<Durability::RecoveredSnapshot> recovered_snapshot;
    for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
      const auto &[path, uuid] = *it;
      if (uuid != uuid_) {
        LOG(WARNING) << "The snapshot file " << path
                     << " isn't related to the latest snapshot file!";
        continue;
      }
      LOG(INFO) << "Starting snapshot recovery from " << path;
      try {
        recovered_snapshot = LoadSnapshot(path);
        LOG(INFO) << "Snapshot recovery successful!";
        break;
      } catch (const RecoveryFailure &e) {
        LOG(WARNING) << "Couldn't recover snapshot from " << path
                     << " because of: " << e.what();
        continue;
      }
    }
    CHECK(recovered_snapshot)
        << "The database is configured to recover on startup, but couldn't "
           "recover using any of the specified snapshots! Please inspect them "
           "and restart the database.";
    recovery_info = recovered_snapshot->recovery_info;
    indices_constraints = std::move(recovered_snapshot->indices_constraints);
    snapshot_timestamp = recovered_snapshot->snapshot_info.start_timestamp;
    if (!utils::DirExists(wal_directory_)) {
      recover_indices_and_constraints(indices_constraints);
      return recovered_snapshot->recovery_info;
    }
  } else {
    if (!utils::DirExists(wal_directory_)) return std::nullopt;
    // Array of all discovered WAL files, ordered by name.
    std::vector<std::pair<std::filesystem::path, std::string>> wal_files;
    for (const auto &item :
         std::filesystem::directory_iterator(wal_directory_, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadWalInfo(item.path());
        wal_files.emplace_back(item.path(), info.uuid);
      } catch (const RecoveryFailure &e) {
        continue;
      }
    }
    CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                       << error_code.message() << "!";
    if (wal_files.empty()) return std::nullopt;
    std::sort(wal_files.begin(), wal_files.end());
    // UUID used for durability is the UUID of the last WAL file.
    uuid_ = wal_files.back().second;
  }

  // Array of all discovered WAL files, ordered by sequence number.
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>>
      wal_files;
  for (const auto &item :
       std::filesystem::directory_iterator(wal_directory_, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if (info.uuid != uuid_) continue;
      wal_files.emplace_back(info.seq_num, info.from_timestamp,
                             info.to_timestamp, item.path());
    } catch (const RecoveryFailure &e) {
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";
  // By this point we should have recovered from a snapshot, or we should have
  // found some WAL files to recover from in the above `else`. This is just a
  // sanity check to circumvent the following case: The database didn't recover
  // from a snapshot, the above `else` triggered to find the recovery UUID from
  // a WAL file. The above `else` has an early exit in case there are no WAL
  // files. Because we reached this point there must have been some WAL files
  // and we must have some WAL files after this second WAL directory iteration.
  CHECK(snapshot_timestamp || !wal_files.empty())
      << "The database didn't recover from a snapshot and didn't find any WAL "
         "files that match the last WAL file!";

  if (!wal_files.empty()) {
    std::sort(wal_files.begin(), wal_files.end());
    {
      const auto &[seq_num, from_timestamp, to_timestamp, path] = wal_files[0];
      if (seq_num != 0) {
        // We don't have all WAL files. We need to see whether we need them all.
        if (!snapshot_timestamp) {
          // We didn't recover from a snapshot and we must have all WAL files
          // starting from the first one (seq_num == 0) to be able to recover
          // data from them.
          LOG(FATAL) << "There are missing prefix WAL files and data can't be "
                        "recovered without them!";
        } else if (to_timestamp >= *snapshot_timestamp) {
          // We recovered from a snapshot and we must have at least one WAL file
          // whose all deltas were created before the snapshot in order to
          // verify that nothing is missing from the beginning of the WAL chain.
          LOG(FATAL) << "You must have at least one WAL file that contains "
                        "deltas that were created before the snapshot file!";
        }
      }
    }
    std::optional<uint64_t> previous_seq_num;
    for (const auto &[seq_num, from_timestamp, to_timestamp, path] :
         wal_files) {
      if (previous_seq_num && *previous_seq_num + 1 != seq_num) {
        LOG(FATAL) << "You are missing a WAL file with the sequence number "
                   << *previous_seq_num + 1 << "!";
      }
      previous_seq_num = seq_num;
      try {
        auto info = LoadWal(path, &indices_constraints, snapshot_timestamp);
        recovery_info.next_vertex_id =
            std::max(recovery_info.next_vertex_id, info.next_vertex_id);
        recovery_info.next_edge_id =
            std::max(recovery_info.next_edge_id, info.next_edge_id);
        recovery_info.next_timestamp =
            std::max(recovery_info.next_timestamp, info.next_timestamp);
      } catch (const RecoveryFailure &e) {
        LOG(FATAL) << "Couldn't recover WAL deltas from " << path
                   << " because of: " << e.what();
      }
    }
    // The sequence number needs to be recovered even though `LoadWal` didn't
    // load any deltas from that file.
    wal_seq_num_ = *previous_seq_num + 1;
  }

  recover_indices_and_constraints(indices_constraints);
  return recovery_info;
}

Durability::RecoveredSnapshot Durability::LoadSnapshot(
    const std::filesystem::path &path) {
  Durability::RecoveryInfo ret;
  RecoveredIndicesAndConstraints indices_constraints;

  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (*version != kVersion) throw RecoveryFailure("Invalid snapshot version!");

  // Cleanup of loaded data in case of failure.
  bool success = false;
  utils::OnScopeExit cleanup([this, &success] {
    if (!success) {
      edges_->clear();
      vertices_->clear();
    }
  });

  // Read snapshot info.
  auto info = ReadSnapshotInfo(path);

  // Check for edges.
  bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    if (!snapshot.SetPosition(info.offset_mapper))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER)
      throw RecoveryFailure("Invalid snapshot data!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Invalid snapshot data!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Invalid snapshot data!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Invalid snapshot data!");
      auto my_id = name_id_mapper_->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end())
      throw RecoveryFailure("Invalid snapshot data!");
    return EdgeTypeId::FromUint(it->second);
  };

  {
    // Recover edges.
    auto edge_acc = edges_->access();
    uint64_t last_edge_gid = 0;
    if (snapshot_has_edges) {
      if (!snapshot.SetPosition(info.offset_edges))
        throw RecoveryFailure("Couldn't read data from snapshot!");
      for (uint64_t i = 0; i < info.edges_count; ++i) {
        {
          auto marker = snapshot.ReadMarker();
          if (!marker || *marker != Marker::SECTION_EDGE)
            throw RecoveryFailure("Invalid snapshot data!");
        }

        if (items_.properties_on_edges) {
          // Insert edge.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid)
            throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;
          auto [it, inserted] =
              edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
          if (!inserted)
            throw RecoveryFailure("The edge must be inserted here!");

          // Recover properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
            auto &props = it->properties;
            for (uint64_t j = 0; j < *props_size; ++j) {
              auto key = snapshot.ReadUint();
              if (!key) throw RecoveryFailure("Invalid snapshot data!");
              auto value = snapshot.ReadPropertyValue();
              if (!value) throw RecoveryFailure("Invalid snapshot data!");
              props.emplace(get_property_from_id(*key), std::move(*value));
            }
          }
        } else {
          // Read edge GID.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid)
            throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;

          // Read properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
            if (*props_size != 0)
              throw RecoveryFailure(
                  "The snapshot has properties on edges, but the storage is "
                  "configured without properties on edges!");
          }
        }
      }
    }

    // Recover vertices (labels and properties).
    if (!snapshot.SetPosition(info.offset_vertices))
      throw RecoveryFailure("Couldn't read data from snapshot!");
    auto vertex_acc = vertices_->access();
    uint64_t last_vertex_gid = 0;
    for (uint64_t i = 0; i < info.vertices_count; ++i) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX)
          throw RecoveryFailure("Invalid snapshot data!");
      }

      // Insert vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (i > 0 && *gid <= last_vertex_gid) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      last_vertex_gid = *gid;
      auto [it, inserted] =
          vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

      // Recover labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &labels = it->labels;
        labels.reserve(*labels_size);
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
          labels.emplace_back(get_label_from_id(*label));
        }
      }

      // Recover properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &props = it->properties;
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.ReadPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
          props.emplace(get_property_from_id(*key), std::move(*value));
        }
      }

      // Skip in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip out edges.
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
      }
    }

    // Recover vertices (in/out edges).
    if (!snapshot.SetPosition(info.offset_vertices))
      throw RecoveryFailure("Couldn't read data from snapshot!");
    for (auto &vertex : vertex_acc) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX)
          throw RecoveryFailure("Invalid snapshot data!");
      }

      // Check vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (gid != vertex.gid.AsUint())
        throw RecoveryFailure("Invalid snapshot data!");

      // Skip labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.SkipPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Recover in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.in_edges.reserve(*in_size);
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

          auto from_vertex = vertex_acc.find(Gid::FromUint(*from_gid));
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("Invalid from vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items_.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end())
                throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] =
                  edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          vertex.in_edges.emplace_back(get_edge_type_from_id(*edge_type),
                                       &*from_vertex, edge_ref);
        }
      }

      // Recover out edges.
      {
        auto out_size = snapshot.ReadUint();
        if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.out_edges.reserve(*out_size);
        for (uint64_t j = 0; j < *out_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto to_gid = snapshot.ReadUint();
          if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

          auto to_vertex = vertex_acc.find(Gid::FromUint(*to_gid));
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("Invalid to vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items_.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end())
                throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] =
                  edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          vertex.out_edges.emplace_back(get_edge_type_from_id(*edge_type),
                                        &*to_vertex, edge_ref);
        }
      }
    }

    // Set initial values for edge/vertex ID generators.
    ret.next_edge_id = last_edge_gid + 1;
    ret.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    if (!snapshot.SetPosition(info.offset_indices))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES)
      throw RecoveryFailure("Invalid snapshot data!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label,
                                    get_label_from_id(*label),
                                    "The label index already exists!");
      }
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(
            &indices_constraints.indices.label_property,
            {get_label_from_id(*label), get_property_from_id(*property)},
            "The label+property index already exists!");
      }
    }
  }

  // Recover constraints.
  {
    if (!snapshot.SetPosition(info.offset_constraints))
      throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Invalid snapshot data!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(
            &indices_constraints.constraints.existence,
            {get_label_from_id(*label), get_property_from_id(*property)},
            "The existence constraint already exists!");
      }
    }
  }

  // Recover timestamp.
  ret.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, ret, std::move(indices_constraints)};
}

Durability::RecoveryInfo Durability::LoadWal(
    const std::filesystem::path &path,
    RecoveredIndicesAndConstraints *indices_constraints,
    std::optional<uint64_t> snapshot_timestamp) {
  Durability::RecoveryInfo ret;

  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version)
    throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (*version != kVersion) throw RecoveryFailure("Invalid WAL version!");

  // Read wal info.
  auto info = ReadWalInfo(path);

  // Check timestamp.
  if (snapshot_timestamp && info.to_timestamp <= *snapshot_timestamp)
    return ret;

  // Recover deltas.
  wal.SetPosition(info.offset_deltas);
  uint64_t deltas_applied = 0;
  auto edge_acc = edges_->access();
  auto vertex_acc = vertices_->access();
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    // Read WAL delta header to find out the delta timestamp.
    auto timestamp = ReadWalDeltaHeader(&wal);

    if (!snapshot_timestamp || timestamp > *snapshot_timestamp) {
      // This delta should be loaded.
      auto delta = ReadWalDeltaData(&wal);
      switch (delta.type) {
        case WalDeltaData::Type::VERTEX_CREATE: {
          auto [vertex, inserted] = vertex_acc.insert(
              Vertex{delta.vertex_create_delete.gid, nullptr});
          if (!inserted)
            throw RecoveryFailure("The vertex must be inserted here!");

          ret.next_vertex_id = std::max(
              ret.next_vertex_id, delta.vertex_create_delete.gid.AsUint() + 1);

          break;
        }
        case WalDeltaData::Type::VERTEX_DELETE: {
          auto vertex = vertex_acc.find(delta.vertex_create_delete.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");
          if (!vertex->in_edges.empty() || !vertex->out_edges.empty())
            throw RecoveryFailure(
                "The vertex can't be deleted because it still has edges!");

          if (!vertex_acc.remove(delta.vertex_create_delete.gid))
            throw RecoveryFailure("The vertex must be removed here!");

          break;
        }
        case WalDeltaData::Type::VERTEX_ADD_LABEL:
        case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
          auto vertex = vertex_acc.find(delta.vertex_add_remove_label.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");

          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.vertex_add_remove_label.label));
          auto it =
              std::find(vertex->labels.begin(), vertex->labels.end(), label_id);

          if (delta.type == WalDeltaData::Type::VERTEX_ADD_LABEL) {
            if (it != vertex->labels.end())
              throw RecoveryFailure("The vertex already has the label!");
            vertex->labels.push_back(label_id);
          } else {
            if (it == vertex->labels.end())
              throw RecoveryFailure("The vertex doesn't have the label!");
            std::swap(*it, vertex->labels.back());
            vertex->labels.pop_back();
          }

          break;
        }
        case WalDeltaData::Type::VERTEX_SET_PROPERTY: {
          auto vertex = vertex_acc.find(delta.vertex_edge_set_property.gid);
          if (vertex == vertex_acc.end())
            throw RecoveryFailure("The vertex doesn't exist!");

          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;

          auto it = vertex->properties.find(property_id);
          if (it != vertex->properties.end()) {
            if (property_value.IsNull()) {
              // remove the property
              vertex->properties.erase(it);
            } else {
              // set the value
              it->second = std::move(property_value);
            }
          } else if (!property_value.IsNull()) {
            vertex->properties.emplace(property_id, std::move(property_value));
          }

          break;
        }
        case WalDeltaData::Type::EDGE_CREATE: {
          auto from_vertex =
              vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(
              name_id_mapper_->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items_.properties_on_edges) {
            auto [edge, inserted] = edge_acc.insert(Edge{edge_gid, nullptr});
            if (!inserted)
              throw RecoveryFailure("The edge must be inserted here!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(),
                                from_vertex->out_edges.end(), link);
            if (it != from_vertex->out_edges.end())
              throw RecoveryFailure("The from vertex already has this edge!");
            from_vertex->out_edges.push_back(link);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(),
                                to_vertex->in_edges.end(), link);
            if (it != to_vertex->in_edges.end())
              throw RecoveryFailure("The to vertex already has this edge!");
            to_vertex->in_edges.push_back(link);
          }

          ret.next_edge_id = std::max(ret.next_edge_id, edge_gid.AsUint() + 1);

          break;
        }
        case WalDeltaData::Type::EDGE_DELETE: {
          auto from_vertex =
              vertex_acc.find(delta.edge_create_delete.from_vertex);
          if (from_vertex == vertex_acc.end())
            throw RecoveryFailure("The from vertex doesn't exist!");
          auto to_vertex = vertex_acc.find(delta.edge_create_delete.to_vertex);
          if (to_vertex == vertex_acc.end())
            throw RecoveryFailure("The to vertex doesn't exist!");

          auto edge_gid = delta.edge_create_delete.gid;
          auto edge_type_id = EdgeTypeId::FromUint(
              name_id_mapper_->NameToId(delta.edge_create_delete.edge_type));
          EdgeRef edge_ref(edge_gid);
          if (items_.properties_on_edges) {
            auto edge = edge_acc.find(edge_gid);
            if (edge == edge_acc.end())
              throw RecoveryFailure("The edge doesn't exist!");
            edge_ref = EdgeRef(&*edge);
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*to_vertex, edge_ref};
            auto it = std::find(from_vertex->out_edges.begin(),
                                from_vertex->out_edges.end(), link);
            if (it == from_vertex->out_edges.end())
              throw RecoveryFailure("The from vertex doesn't have this edge!");
            std::swap(*it, from_vertex->out_edges.back());
            from_vertex->out_edges.pop_back();
          }
          {
            std::tuple<EdgeTypeId, Vertex *, EdgeRef> link{
                edge_type_id, &*from_vertex, edge_ref};
            auto it = std::find(to_vertex->in_edges.begin(),
                                to_vertex->in_edges.end(), link);
            if (it == to_vertex->in_edges.end())
              throw RecoveryFailure("The to vertex doesn't have this edge!");
            std::swap(*it, to_vertex->in_edges.back());
            to_vertex->in_edges.pop_back();
          }
          if (items_.properties_on_edges) {
            if (!edge_acc.remove(edge_gid))
              throw RecoveryFailure("The edge must be removed here!");
          }

          break;
        }
        case WalDeltaData::Type::EDGE_SET_PROPERTY: {
          if (!items_.properties_on_edges)
            throw RecoveryFailure(
                "The WAL has properties on edges, but the storage is "
                "configured without properties on edges!");
          auto edge = edge_acc.find(delta.vertex_edge_set_property.gid);
          if (edge == edge_acc.end())
            throw RecoveryFailure("The edge doesn't exist!");
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.vertex_edge_set_property.property));
          auto &property_value = delta.vertex_edge_set_property.value;
          auto it = edge->properties.find(property_id);
          if (it != edge->properties.end()) {
            if (property_value.IsNull()) {
              // remove the property
              edge->properties.erase(it);
            } else {
              // set the value
              it->second = std::move(property_value);
            }
          } else if (!property_value.IsNull()) {
            edge->properties.emplace(property_id, std::move(property_value));
          }
          break;
        }
        case WalDeltaData::Type::TRANSACTION_END:
          break;
        case WalDeltaData::Type::LABEL_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label.label));
          AddRecoveredIndexConstraint(&indices_constraints->indices.label,
                                      label_id,
                                      "The label index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_INDEX_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label.label));
          RemoveRecoveredIndexConstraint(&indices_constraints->indices.label,
                                         label_id,
                                         "The label index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          AddRecoveredIndexConstraint(
              &indices_constraints->indices.label_property,
              {label_id, property_id},
              "The label property index already exists!");
          break;
        }
        case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(
              &indices_constraints->indices.label_property,
              {label_id, property_id},
              "The label property index doesn't exist!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          AddRecoveredIndexConstraint(
              &indices_constraints->constraints.existence,
              {label_id, property_id},
              "The existence constraint already exists!");
          break;
        }
        case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
          auto label_id = LabelId::FromUint(
              name_id_mapper_->NameToId(delta.operation_label_property.label));
          auto property_id = PropertyId::FromUint(name_id_mapper_->NameToId(
              delta.operation_label_property.property));
          RemoveRecoveredIndexConstraint(
              &indices_constraints->constraints.existence,
              {label_id, property_id},
              "The existence constraint doesn't exist!");
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

  LOG(INFO) << "Applied " << deltas_applied << " deltas from WAL " << path;

  return ret;
}

bool Durability::InitializeWalFile() {
  if (config_.snapshot_wal_mode !=
      Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, items_, name_id_mapper_,
                      wal_seq_num_++);
  }
  return true;
}

void Durability::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >= config_.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >= config_.wal_file_size_kibibytes) {
    wal_file_ = std::nullopt;
    wal_unsynced_transactions_ = 0;
  }
}

}  // namespace storage
