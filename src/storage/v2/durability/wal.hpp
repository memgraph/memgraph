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

#include <cstdint>
#include <filesystem>
#include <set>
#include <string>

#include "storage/v2/config.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/storage_global_operation.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/file_locker.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage::durability {

/// Structure used to hold information about a WAL.
struct WalInfo {
  uint64_t offset_metadata;
  uint64_t offset_deltas;

  std::string uuid;
  std::string epoch_id;
  uint64_t seq_num;
  uint64_t from_timestamp;
  uint64_t to_timestamp;
  uint64_t num_deltas;
};

/// Structure used to return loaded WAL delta data.
struct WalDeltaData {
  enum class Type {
    VERTEX_CREATE,
    VERTEX_DELETE,
    VERTEX_ADD_LABEL,
    VERTEX_REMOVE_LABEL,
    VERTEX_SET_PROPERTY,
    EDGE_CREATE,
    EDGE_DELETE,
    EDGE_SET_PROPERTY,
    TRANSACTION_END,
    LABEL_INDEX_CREATE,
    LABEL_INDEX_DROP,
    LABEL_INDEX_STATS_SET,
    LABEL_INDEX_STATS_CLEAR,
    LABEL_PROPERTY_INDEX_CREATE,
    LABEL_PROPERTY_INDEX_DROP,
    LABEL_PROPERTY_INDEX_STATS_SET,
    LABEL_PROPERTY_INDEX_STATS_CLEAR,
    EXISTENCE_CONSTRAINT_CREATE,
    EXISTENCE_CONSTRAINT_DROP,
    UNIQUE_CONSTRAINT_CREATE,
    UNIQUE_CONSTRAINT_DROP,
  };

  Type type{Type::TRANSACTION_END};

  struct {
    Gid gid;
  } vertex_create_delete;

  struct {
    Gid gid;
    std::string label;
  } vertex_add_remove_label;

  struct {
    Gid gid;
    std::string property;
    PropertyValue value;
  } vertex_edge_set_property;

  struct {
    Gid gid;
    std::string edge_type;
    Gid from_vertex;
    Gid to_vertex;
  } edge_create_delete;

  struct {
    std::string label;
  } operation_label;

  struct {
    std::string label;
    std::string property;
  } operation_label_property;

  struct {
    std::string label;
    std::set<std::string, std::less<>> properties;
  } operation_label_properties;

  struct {
    std::string label;
    std::string stats;
  } operation_label_stats;

  struct {
    std::string label;
    std::string property;
    std::string stats;
  } operation_label_property_stats;
};

bool operator==(const WalDeltaData &a, const WalDeltaData &b);
bool operator!=(const WalDeltaData &a, const WalDeltaData &b);

constexpr bool IsWalDeltaDataTypeTransactionEndVersion15(const WalDeltaData::Type type) {
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
    case WalDeltaData::Type::LABEL_INDEX_STATS_SET:
    case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET:
    case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE:
    case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP:
      return true;  // TODO: Still true?
  }
}

constexpr bool IsWalDeltaDataTypeTransactionEnd(const WalDeltaData::Type type, const uint64_t version = kVersion) {
  if (version < 17U) {
    return IsWalDeltaDataTypeTransactionEndVersion15(type);
  }
  // All deltas are now handled in a transactional scope
  return type == WalDeltaData::Type::TRANSACTION_END;
}

/// Function used to read information about the WAL file.
/// @throw RecoveryFailure
WalInfo ReadWalInfo(const std::filesystem::path &path);

/// Function used to read the WAL delta header. The function returns the delta
/// timestamp.
/// @throw RecoveryFailure
uint64_t ReadWalDeltaHeader(BaseDecoder *decoder);

/// Function used to read the current WAL delta data. The function returns the
/// read delta data. The WAL delta header must be read before calling this
/// function.
/// @throw RecoveryFailure
WalDeltaData ReadWalDeltaData(BaseDecoder *decoder);

/// Function used to skip the current WAL delta data. The function returns the
/// skipped delta type. The WAL delta header must be read before calling this
/// function.
/// @throw RecoveryFailure
WalDeltaData::Type SkipWalDeltaData(BaseDecoder *decoder);

/// Function used to encode a `Delta` that originated from a `Vertex`.
void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, Config::Items items, const Delta &delta,
                 const Vertex &vertex, uint64_t timestamp);

/// Function used to encode a `Delta` that originated from an `Edge`.
void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, const Delta &delta, const Edge &edge,
                 uint64_t timestamp);

/// Function used to encode the transaction end.
void EncodeTransactionEnd(BaseEncoder *encoder, uint64_t timestamp);

/// Function used to encode non-transactional operation.
void EncodeOperation(BaseEncoder *encoder, NameIdMapper *name_id_mapper, StorageMetadataOperation operation,
                     LabelId label, const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                     const LabelPropertyIndexStats &property_stats, uint64_t timestamp);

/// Function used to load the WAL data into the storage.
/// @throw RecoveryFailure
RecoveryInfo LoadWal(const std::filesystem::path &path, RecoveredIndicesAndConstraints *indices_constraints,
                     std::optional<uint64_t> last_loaded_timestamp, utils::SkipList<Vertex> *vertices,
                     utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                     Config::Items items);

/// WalFile class used to append deltas and operations to the WAL file.
class WalFile {
 public:
  WalFile(const std::filesystem::path &wal_directory, std::string_view uuid, std::string_view epoch_id,
          Config::Items items, NameIdMapper *name_id_mapper, uint64_t seq_num, utils::FileRetainer *file_retainer);
  WalFile(std::filesystem::path current_wal_path, Config::Items items, NameIdMapper *name_id_mapper, uint64_t seq_num,
          uint64_t from_timestamp, uint64_t to_timestamp, uint64_t count, utils::FileRetainer *file_retainer);

  WalFile(const WalFile &) = delete;
  WalFile(WalFile &&) = delete;
  WalFile &operator=(const WalFile &) = delete;
  WalFile &operator=(WalFile &&) = delete;

  ~WalFile();

  void AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t timestamp);
  void AppendDelta(const Delta &delta, const Edge &edge, uint64_t timestamp);

  void AppendTransactionEnd(uint64_t timestamp);

  void AppendOperation(StorageMetadataOperation operation, LabelId label, const std::set<PropertyId> &properties,
                       const LabelIndexStats &stats, const LabelPropertyIndexStats &property_stats, uint64_t timestamp);

  void Sync();

  uint64_t GetSize();

  uint64_t SequenceNumber() const;

  auto FromTimestamp() const { return from_timestamp_; }

  auto ToTimestamp() const { return to_timestamp_; }

  auto Count() const { return count_; }

  // Disable flushing of the internal buffer.
  void DisableFlushing();
  // Enable flushing of the internal buffer.
  void EnableFlushing();
  // Try flushing the internal buffer.
  void TryFlushing();
  // Get the internal buffer with its size.
  std::pair<const uint8_t *, size_t> CurrentFileBuffer() const;

  // Get the path of the current WAL file.
  const auto &Path() const { return path_; }

  void FinalizeWal();
  void DeleteWal();

 private:
  void UpdateStats(uint64_t timestamp);

  Config::Items items_;
  NameIdMapper *name_id_mapper_;
  Encoder wal_;
  std::filesystem::path path_;
  uint64_t from_timestamp_;
  uint64_t to_timestamp_;
  uint64_t count_;
  uint64_t seq_num_;

  utils::FileRetainer *file_retainer_;
};

}  // namespace memgraph::storage::durability
