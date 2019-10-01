#include "storage/v2/durability.hpp"

#include <algorithm>
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
    case Marker::SECTION_OFFSETS:
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
    case Marker::SECTION_OFFSETS:
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
// Magic values written to the start of a snapshot/WAL file to identify it.
const std::string kSnapshotMagic{"MGsn"};
const std::string kWalMagic{"MGwl"};

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
      uuid_(utils::GenerateUUID()) {}

std::optional<Durability::RecoveryInfo> Durability::Initialize(
    std::function<void(std::function<void(Transaction *)>)>
        execute_with_transaction) {
  execute_with_transaction_ = execute_with_transaction;
  std::optional<Durability::RecoveryInfo> ret;
  if (config_.recover_on_startup) {
    ret = RecoverData();
  }
  if (config_.snapshot_type == Config::Durability::SnapshotType::PERIODIC ||
      config_.snapshot_on_exit) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
  }
  if (config_.snapshot_type == Config::Durability::SnapshotType::PERIODIC) {
    snapshot_runner_.Run("Snapshot", config_.snapshot_interval, [this] {
      execute_with_transaction_(
          [this](Transaction *transaction) { CreateSnapshot(transaction); });
    });
  }
  return ret;
}

void Durability::Finalize() {
  if (config_.snapshot_type == Config::Durability::SnapshotType::PERIODIC) {
    snapshot_runner_.Stop();
  }
  if (config_.snapshot_on_exit) {
    execute_with_transaction_(
        [this](Transaction *transaction) { CreateSnapshot(transaction); });
  }
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
  std::vector<std::filesystem::path> old_snapshot_files;
  std::error_code error_code;
  for (auto &item :
       std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
    if (!item.is_regular_file()) continue;
    if (item.path() == path) continue;
    try {
      auto info = ReadSnapshotInfo(item.path());
      if (info.uuid == uuid_) {
        old_snapshot_files.push_back(item.path());
      }
    } catch (const RecoveryFailure &e) {
      LOG(WARNING) << "Found a corrupt snapshot file " << item.path()
                   << " because of: " << e.what();
      continue;
    }
  }
  if (error_code) {
    LOG(ERROR) << "Couldn't ensure that exactly "
               << config_.snapshot_retention_count
               << " snapshots exist because an error occurred: "
               << error_code.message() << "!";
  }
  if (old_snapshot_files.size() >= config_.snapshot_retention_count) {
    std::sort(old_snapshot_files.begin(), old_snapshot_files.end());
    for (size_t i = 0;
         i <= old_snapshot_files.size() - config_.snapshot_retention_count;
         ++i) {
      const auto &path = old_snapshot_files[i];
      if (!utils::DeleteFile(path)) {
        LOG(WARNING) << "Couldn't delete snapshot file " << path << "!";
      }
    }
  }
}

std::optional<Durability::RecoveryInfo> Durability::RecoverData() {
  if (!utils::DirExists(snapshot_directory_)) return std::nullopt;

  // Array of all discovered snapshots, ordered by name.
  std::vector<std::filesystem::path> snapshot_files;
  std::error_code error_code;
  for (auto &item :
       std::filesystem::directory_iterator(snapshot_directory_, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      ReadSnapshotInfo(item.path());
      snapshot_files.push_back(item.path());
    } catch (const RecoveryFailure &) {
      continue;
    }
  }
  CHECK(!error_code) << "Couldn't recover data because an error occurred: "
                     << error_code.message() << "!";
  std::sort(snapshot_files.begin(), snapshot_files.end());
  for (auto it = snapshot_files.rbegin(); it != snapshot_files.rend(); ++it) {
    const auto &path = *it;
    LOG(INFO) << "Starting snapshot recovery from " << path;
    try {
      auto info = LoadSnapshot(path);
      LOG(INFO) << "Snapshot recovery successful!";
      return info;
    } catch (const RecoveryFailure &e) {
      LOG(WARNING) << "Couldn't recover snapshot from " << path
                   << " because of: " << e.what();
      continue;
    }
  }
  return std::nullopt;
}

Durability::RecoveryInfo Durability::LoadSnapshot(
    const std::filesystem::path &path) {
  Durability::RecoveryInfo ret;

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
      indices_->label_index.Clear();
      indices_->label_property_index.Clear();
      constraints_->existence_constraints.clear();
    }
  });

  // Read snapshot info.
  auto info = ReadSnapshotInfo(path);

  // Check for edges.
  bool snapshot_has_edges = info.offset_edges != 0;

  // Set storage UUID.
  uuid_ = info.uuid;

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
        if (!indices_->label_index.CreateIndex(get_label_from_id(*label),
                                               vertices_->access()))
          throw RecoveryFailure("Couldn't recover label index!");
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
        if (!indices_->label_property_index.CreateIndex(
                get_label_from_id(*label), get_property_from_id(*property),
                vertices_->access()))
          throw RecoveryFailure("Couldn't recover label+property index!");
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
        auto ret = CreateExistenceConstraint(
            constraints_, get_label_from_id(*label),
            get_property_from_id(*property), vertices_->access());
        if (!ret.HasValue() || !*ret)
          throw RecoveryFailure("Couldn't recover existence constraint!");
      }
    }
  }

  // Recover timestamp.
  ret.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return ret;
}

}  // namespace storage
