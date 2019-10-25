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
#include "storage/v2/delta.hpp"
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
  SECTION_DELTA = 0x26,
  SECTION_OFFSETS = 0x42,

  DELTA_VERTEX_CREATE = 0x50,
  DELTA_VERTEX_DELETE = 0x51,
  DELTA_VERTEX_ADD_LABEL = 0x52,
  DELTA_VERTEX_REMOVE_LABEL = 0x53,
  DELTA_VERTEX_SET_PROPERTY = 0x54,
  DELTA_EDGE_CREATE = 0x55,
  DELTA_EDGE_DELETE = 0x56,
  DELTA_EDGE_SET_PROPERTY = 0x57,
  DELTA_TRANSACTION_END = 0x58,
  DELTA_LABEL_INDEX_CREATE = 0x59,
  DELTA_LABEL_INDEX_DROP = 0x5a,
  DELTA_LABEL_PROPERTY_INDEX_CREATE = 0x5b,
  DELTA_LABEL_PROPERTY_INDEX_DROP = 0x5c,
  DELTA_EXISTENCE_CONSTRAINT_CREATE = 0x5d,
  DELTA_EXISTENCE_CONSTRAINT_DROP = 0x5e,

  VALUE_FALSE = 0x00,
  VALUE_TRUE = 0xff,
};

/// List of all available markers.
/// IMPORTANT: Don't forget to update this list when you add a new Marker.
static const Marker kMarkersAll[] = {
    Marker::TYPE_NULL,
    Marker::TYPE_BOOL,
    Marker::TYPE_INT,
    Marker::TYPE_DOUBLE,
    Marker::TYPE_STRING,
    Marker::TYPE_LIST,
    Marker::TYPE_MAP,
    Marker::TYPE_PROPERTY_VALUE,
    Marker::SECTION_VERTEX,
    Marker::SECTION_EDGE,
    Marker::SECTION_MAPPER,
    Marker::SECTION_METADATA,
    Marker::SECTION_INDICES,
    Marker::SECTION_CONSTRAINTS,
    Marker::SECTION_DELTA,
    Marker::SECTION_OFFSETS,
    Marker::DELTA_VERTEX_CREATE,
    Marker::DELTA_VERTEX_DELETE,
    Marker::DELTA_VERTEX_ADD_LABEL,
    Marker::DELTA_VERTEX_REMOVE_LABEL,
    Marker::DELTA_VERTEX_SET_PROPERTY,
    Marker::DELTA_EDGE_CREATE,
    Marker::DELTA_EDGE_DELETE,
    Marker::DELTA_EDGE_SET_PROPERTY,
    Marker::DELTA_TRANSACTION_END,
    Marker::DELTA_LABEL_INDEX_CREATE,
    Marker::DELTA_LABEL_INDEX_DROP,
    Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE,
    Marker::DELTA_LABEL_PROPERTY_INDEX_DROP,
    Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE,
    Marker::DELTA_EXISTENCE_CONSTRAINT_DROP,
    Marker::VALUE_FALSE,
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

  void Sync();

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

/// Structure used to hold information about a WAL.
struct WalInfo {
  uint64_t offset_metadata;
  uint64_t offset_deltas;

  std::string uuid;
  uint64_t seq_num;
  uint64_t from_timestamp;
  uint64_t to_timestamp;
  uint64_t num_deltas;
};

/// Function used to read information about the WAL file.
/// @throw RecoveryFailure
WalInfo ReadWalInfo(const std::filesystem::path &path);

/// Enum used to indicate a global database operation that isn't transactional.
enum class StorageGlobalOperation {
  LABEL_INDEX_CREATE,
  LABEL_INDEX_DROP,
  LABEL_PROPERTY_INDEX_CREATE,
  LABEL_PROPERTY_INDEX_DROP,
  EXISTENCE_CONSTRAINT_CREATE,
  EXISTENCE_CONSTRAINT_DROP,
};

/// WalFile class used to append deltas and operations to the WAL file.
class WalFile {
 public:
  WalFile(const std::filesystem::path &wal_directory, const std::string &uuid,
          Config::Items items, NameIdMapper *name_id_mapper, uint64_t seq_num);

  WalFile(const WalFile &) = delete;
  WalFile(WalFile &&) = delete;
  WalFile &operator=(const WalFile &) = delete;
  WalFile &operator=(WalFile &&) = delete;

  ~WalFile();

  void AppendDelta(const Delta &delta, Vertex *vertex, uint64_t timestamp);
  void AppendDelta(const Delta &delta, Edge *edge, uint64_t timestamp);

  void AppendTransactionEnd(uint64_t timestamp);

  void AppendOperation(StorageGlobalOperation operation, LabelId label,
                       std::optional<PropertyId> property, uint64_t timestamp);

  void Sync();

  uint64_t GetSize();

 private:
  void UpdateStats(uint64_t timestamp);

  Config::Items items_;
  NameIdMapper *name_id_mapper_;
  Encoder wal_;
  std::filesystem::path path_;
  uint64_t from_timestamp_;
  uint64_t to_timestamp_;
  uint64_t count_;
};

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
