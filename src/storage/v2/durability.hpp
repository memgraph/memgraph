#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"

namespace storage {

static const std::string kSnapshotDirectory{"snapshots"};
static const std::string kWalDirectory{"wal"};

static_assert(std::is_same_v<uint8_t, unsigned char>);

/// Markers that are used to indicate crucial parts of the snapshot/WAL.
/// IMPORTANT: Don't forget to update the list of all markers `kMarkersAll` when
/// you add a new Marker.
enum class Marker : uint8_t {
  TYPE_NULL = 0x10,
  TYPE_BOOL = 0x11,
  TYPE_INT = 0x12,
  TYPE_DOUBLE = 0x13,
  TYPE_STRING = 0x14,
  TYPE_LIST = 0x15,
  TYPE_MAP = 0x16,
  TYPE_PROPERTY_VALUE = 0x17,

  SECTION_VERTEX = 0x20,
  SECTION_EDGE = 0x21,
  SECTION_MAPPER = 0x22,
  SECTION_METADATA = 0x23,
  SECTION_INDICES = 0x24,
  SECTION_CONSTRAINTS = 0x25,
  SECTION_OFFSETS = 0x42,

  VALUE_FALSE = 0x00,
  VALUE_TRUE = 0xff,
};

/// List of all available markers.
/// IMPORTANT: Don't forget to update this list when you add a new Marker.
static const Marker kMarkersAll[] = {
    Marker::TYPE_NULL,       Marker::TYPE_BOOL,
    Marker::TYPE_INT,        Marker::TYPE_DOUBLE,
    Marker::TYPE_STRING,     Marker::TYPE_LIST,
    Marker::TYPE_MAP,        Marker::TYPE_PROPERTY_VALUE,
    Marker::SECTION_VERTEX,  Marker::SECTION_EDGE,
    Marker::SECTION_MAPPER,  Marker::SECTION_METADATA,
    Marker::SECTION_INDICES, Marker::SECTION_CONSTRAINTS,
    Marker::SECTION_OFFSETS, Marker::VALUE_FALSE,
    Marker::VALUE_TRUE,
};

/// Encoder that is used to generate a snapshot/WAL.
class Encoder final {
 public:
  void Initialize(const std::filesystem::path &path,
                  const std::string_view &magic, uint64_t version);

  // Main write function, the only one that is allowed to write to the `file_`
  // directly.
  void Write(const uint8_t *data, uint64_t size);

  void WriteMarker(Marker marker);
  void WriteBool(bool value);
  void WriteUint(uint64_t value);
  void WriteDouble(double value);
  void WriteString(const std::string_view &value);
  void WritePropertyValue(const PropertyValue &value);

  uint64_t GetPosition();
  void SetPosition(uint64_t position);

  void Finalize();

 private:
  utils::OutputFile file_;
};

/// Decoder that is used to read a generated snapshot/WAL.
class Decoder final {
 public:
  std::optional<uint64_t> Initialize(const std::filesystem::path &path,
                                     const std::string &magic);

  // Main read functions, the only one that are allowed to read from the `file_`
  // directly.
  bool Read(uint8_t *data, size_t size);
  bool Peek(uint8_t *data, size_t size);

  std::optional<Marker> PeekMarker();

  std::optional<Marker> ReadMarker();
  std::optional<bool> ReadBool();
  std::optional<uint64_t> ReadUint();
  std::optional<double> ReadDouble();
  std::optional<std::string> ReadString();
  std::optional<PropertyValue> ReadPropertyValue();

  bool SkipString();
  bool SkipPropertyValue();

  std::optional<uint64_t> GetSize();
  std::optional<uint64_t> GetPosition();
  bool SetPosition(uint64_t position);

 private:
  utils::InputFile file_;
};

/// Exception used to handle errors during recovery.
class RecoveryFailure : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/// Structure used to hold information about a snapshot.
struct SnapshotInfo {
  uint64_t offset_edges;
  uint64_t offset_vertices;
  uint64_t offset_indices;
  uint64_t offset_constraints;
  uint64_t offset_mapper;
  uint64_t offset_metadata;

  std::string uuid;
  uint64_t start_timestamp;
  uint64_t edges_count;
  uint64_t vertices_count;
};

/// Function used to read information about the snapshot file.
/// @throw RecoveryFailure
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path);

/// Durability class that is used to provide full durability functionality to
/// the storage.
class Durability final {
 public:
  struct RecoveryInfo {
    uint64_t next_vertex_id;
    uint64_t next_edge_id;
    uint64_t next_timestamp;
  };

  Durability(Config::Durability config, utils::SkipList<Vertex> *vertices,
             utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper,
             Indices *indices, Constraints *constraints, Config::Items items);

  std::optional<RecoveryInfo> Initialize(
      std::function<void(std::function<void(Transaction *)>)>
          execute_with_transaction);

  void Finalize();

 private:
  void CreateSnapshot(Transaction *transaction);

  std::optional<RecoveryInfo> RecoverData();

  RecoveryInfo LoadSnapshot(const std::filesystem::path &path);

  Config::Durability config_;

  utils::SkipList<Vertex> *vertices_;
  utils::SkipList<Edge> *edges_;
  NameIdMapper *name_id_mapper_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items items_;

  std::function<void(std::function<void(Transaction *)>)>
      execute_with_transaction_;

  std::filesystem::path snapshot_directory_;
  utils::Scheduler snapshot_runner_;

  // UUID used to distinguish snapshots and to link snapshots to WALs
  std::string uuid_;
};

}  // namespace storage
