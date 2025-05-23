// Copyright 2025 Memgraph Ltd.
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
#include "storage/v2/schema_info.hpp"
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

template <auto MIN_VER, typename Type>
struct VersionDependant {};

template <auto MIN_VER, typename Before, typename After, auto Upgrader>
struct VersionDependantUpgradable {};

using PropertyPathStr = std::vector<std::vector<std::string>>;

constexpr auto SinglePropertyToVector = [](std::string v) { return std::vector{v}; };

constexpr auto PropertyVectorToPropertyPaths = [](std::vector<std::string> v) -> PropertyPathStr {
  PropertyPathStr result;
  for (const auto &property : v) {
    result.emplace_back(std::vector{property});
  }
  return result;
};

using UpgradableSingleProperty = VersionDependantUpgradable<kCompositeIndicesForLabelProperties, std::string,
                                                            std::vector<std::string>, SinglePropertyToVector>;

using UpgradablePropertyPaths =
    VersionDependantUpgradable<kCompositeIndicesForLabelProperties, UpgradableSingleProperty, PropertyPathStr,
                               PropertyVectorToPropertyPaths>;

// Common structures used by more than one WAL Delta
struct VertexOpInfo {
  friend bool operator==(const VertexOpInfo &, const VertexOpInfo &) = default;
  using ctr_types = std::tuple<Gid>;
  Gid gid;
};
struct VertexLabelOpInfo {
  friend bool operator==(const VertexLabelOpInfo &, const VertexLabelOpInfo &) = default;
  using ctr_types = std::tuple<Gid, std::string>;
  Gid gid;
  std::string label;
};
struct EdgeOpInfo {
  friend bool operator==(const EdgeOpInfo &, const EdgeOpInfo &) = default;
  using ctr_types = std::tuple<Gid, std::string, Gid, Gid>;
  Gid gid;
  std::string edge_type;
  Gid from_vertex;
  Gid to_vertex;
};
struct LabelOpInfo {
  friend bool operator==(const LabelOpInfo &, const LabelOpInfo &) = default;
  using ctr_types = std::tuple<std::string>;
  std::string label;
};
struct LabelPropertyOpInfo {
  friend bool operator==(const LabelPropertyOpInfo &, const LabelPropertyOpInfo &) = default;
  using ctr_types = std::tuple<std::string, std::string>;
  std::string label;
  std::string property;
};
struct LabelOrderedPropertiesOpInfo {
  friend bool operator==(const LabelOrderedPropertiesOpInfo &, const LabelOrderedPropertiesOpInfo &) = default;
  using ctr_types = std::tuple<std::string, UpgradablePropertyPaths>;
  std::string label;
  PropertyPathStr properties;
};

struct LabelUnorderedPropertiesOpInfo {
  friend bool operator==(const LabelUnorderedPropertiesOpInfo &, const LabelUnorderedPropertiesOpInfo &) = default;
  using ctr_types = std::tuple<std::string, std::set<std::string, std::less<>>>;
  std::string label;
  std::set<std::string, std::less<>> properties;
};
struct EdgeTypeOpInfo {
  friend bool operator==(const EdgeTypeOpInfo &, const EdgeTypeOpInfo &) = default;
  using ctr_types = std::tuple<std::string>;
  std::string edge_type;
};
struct EdgeTypePropertyOpInfo {
  friend bool operator==(const EdgeTypePropertyOpInfo &, const EdgeTypePropertyOpInfo &) = default;
  using ctr_types = std::tuple<std::string, std::string>;
  std::string edge_type;
  std::string property;
};
struct EdgePropertyOpInfo {
  friend bool operator==(const EdgePropertyOpInfo &, const EdgePropertyOpInfo &) = default;
  using ctr_types = std::tuple<std::string>;
  std::string property;
};
struct TypeConstraintOpInfo {
  friend bool operator==(const TypeConstraintOpInfo &, const TypeConstraintOpInfo &) = default;
  using ctr_types = std::tuple<std::string, std::string, TypeConstraintKind>;
  std::string label;
  std::string property;
  TypeConstraintKind kind;
};
struct TextIndexOpInfo {
  friend bool operator==(const TextIndexOpInfo &, const TextIndexOpInfo &) = default;
  using ctr_types = std::tuple<std::string, std::string>;
  std::string index_name;
  std::string label;
};

// Wal Deltas after Decode
struct WalVertexCreate : VertexOpInfo {};
struct WalVertexDelete : VertexOpInfo {};
struct WalVertexAddLabel : VertexLabelOpInfo {};
struct WalVertexRemoveLabel : VertexLabelOpInfo {};
struct WalVertexSetProperty {
  friend bool operator==(const WalVertexSetProperty &, const WalVertexSetProperty &) = default;
  using ctr_types = std::tuple<Gid, std::string, ExternalPropertyValue>;
  Gid gid;
  std::string property;
  ExternalPropertyValue value;
};
struct WalEdgeSetProperty {
  friend bool operator==(const WalEdgeSetProperty &lhs, const WalEdgeSetProperty &rhs) {
    // Since kEdgeSetDeltaWithVertexInfo version delta holds from vertex gid; this is an extra information (no need to
    // check it)
    return std::tie(lhs.gid, lhs.property, lhs.value) == std::tie(rhs.gid, rhs.property, rhs.value);
  }
  using ctr_types =
      std::tuple<Gid, std::string, ExternalPropertyValue, VersionDependant<kEdgeSetDeltaWithVertexInfo, Gid>>;
  Gid gid;
  std::string property;
  ExternalPropertyValue value;
  std::optional<Gid> from_gid;  //!< Used to simplify the edge search (from kEdgeSetDeltaWithVertexInfo)
};
struct WalEdgeCreate : EdgeOpInfo {};
struct WalEdgeDelete : EdgeOpInfo {};
struct WalTransactionEnd {
  friend bool operator==(const WalTransactionEnd &, const WalTransactionEnd &) = default;
  using ctr_types = std::tuple<>;
};
struct WalLabelIndexCreate : LabelOpInfo {};
struct WalLabelIndexDrop : LabelOpInfo {};
struct WalLabelIndexStatsClear : LabelOpInfo {};
struct WalLabelPropertyIndexStatsClear : LabelOpInfo {
};  // Special case, this clear is done on all label/property pairs that contain the defined label
struct WalEdgeTypeIndexCreate : EdgeTypeOpInfo {};
struct WalEdgeTypeIndexDrop : EdgeTypeOpInfo {};
struct WalLabelIndexStatsSet {
  friend bool operator==(const WalLabelIndexStatsSet &, const WalLabelIndexStatsSet &) = default;
  using ctr_types = std::tuple<std::string, std::string>;
  std::string label;
  std::string json_stats;
};
struct WalLabelPropertyIndexCreate : LabelOrderedPropertiesOpInfo {};
struct WalLabelPropertyIndexDrop : LabelOrderedPropertiesOpInfo {};
struct WalPointIndexCreate : LabelPropertyOpInfo {};
struct WalPointIndexDrop : LabelPropertyOpInfo {};
struct WalExistenceConstraintCreate : LabelPropertyOpInfo {};
struct WalExistenceConstraintDrop : LabelPropertyOpInfo {};
struct WalLabelPropertyIndexStatsSet {
  friend bool operator==(const WalLabelPropertyIndexStatsSet &, const WalLabelPropertyIndexStatsSet &) = default;
  using ctr_types = std::tuple<std::string, UpgradablePropertyPaths, std::string>;
  std::string label;
  PropertyPathStr properties;
  std::string json_stats;
};
struct WalEdgeTypePropertyIndexCreate : EdgeTypePropertyOpInfo {};
struct WalEdgeTypePropertyIndexDrop : EdgeTypePropertyOpInfo {};
struct WalEdgePropertyIndexCreate : EdgePropertyOpInfo {};
struct WalEdgePropertyIndexDrop : EdgePropertyOpInfo {};
struct WalUniqueConstraintCreate : LabelUnorderedPropertiesOpInfo {};
struct WalUniqueConstraintDrop : LabelUnorderedPropertiesOpInfo {};
struct WalTypeConstraintCreate : TypeConstraintOpInfo {};
struct WalTypeConstraintDrop : TypeConstraintOpInfo {};
struct WalTextIndexCreate : TextIndexOpInfo {};
struct WalTextIndexDrop : TextIndexOpInfo {};
struct WalEnumCreate {
  friend bool operator==(const WalEnumCreate &, const WalEnumCreate &) = default;
  using ctr_types = std::tuple<std::string, std::vector<std::string>>;
  std::string etype;
  std::vector<std::string> evalues;
};
struct WalEnumAlterAdd {
  friend bool operator==(const WalEnumAlterAdd &, const WalEnumAlterAdd &) = default;
  using ctr_types = std::tuple<std::string, std::string>;
  std::string etype;
  std::string evalue;
};
struct WalEnumAlterUpdate {
  friend bool operator==(const WalEnumAlterUpdate &, const WalEnumAlterUpdate &) = default;
  using ctr_types = std::tuple<std::string, std::string, std::string>;
  std::string etype;
  std::string evalue_old;
  std::string evalue_new;
};
struct WalVectorIndexCreate {
  friend bool operator==(const WalVectorIndexCreate &, const WalVectorIndexCreate &) = default;
  using ctr_types =
      std::tuple<std::string, std::string, std::string, std::string, std::uint16_t, std::uint16_t, std::size_t>;
  std::string index_name;
  std::string label;
  std::string property;
  std::string metric_kind;
  std::uint16_t dimension;
  std::uint16_t resize_coefficient;
  std::size_t capacity;
};
struct WalVectorIndexDrop {
  friend bool operator==(const WalVectorIndexDrop &, const WalVectorIndexDrop &) = default;
  using ctr_types = std::tuple<std::string>;
  std::string index_name;
};

/// Structure used to return loaded WAL delta data.
struct WalDeltaData {
  friend bool operator==(const WalDeltaData &a, const WalDeltaData &b) {
    return std::visit(utils::Overloaded{
                          []<typename T>(T const &lhs, T const &rhs) { return lhs == rhs; },
                          [](auto const &, auto const &) { return false; },

                      },
                      a.data_, b.data_);
  }

  std::variant<WalVertexCreate, WalVertexDelete, WalVertexAddLabel, WalVertexRemoveLabel, WalVertexSetProperty,
               WalEdgeSetProperty, WalEdgeCreate, WalEdgeDelete, WalTransactionEnd, WalLabelIndexCreate,
               WalLabelIndexDrop, WalLabelIndexStatsClear, WalLabelPropertyIndexStatsClear, WalEdgeTypeIndexCreate,
               WalEdgeTypeIndexDrop, WalEdgePropertyIndexCreate, WalEdgePropertyIndexDrop, WalLabelIndexStatsSet,
               WalLabelPropertyIndexCreate, WalLabelPropertyIndexDrop, WalPointIndexCreate, WalPointIndexDrop,
               WalExistenceConstraintCreate, WalExistenceConstraintDrop, WalLabelPropertyIndexStatsSet,
               WalEdgeTypePropertyIndexCreate, WalEdgeTypePropertyIndexDrop, WalUniqueConstraintCreate,
               WalUniqueConstraintDrop, WalTypeConstraintCreate, WalTypeConstraintDrop, WalTextIndexCreate,
               WalTextIndexDrop, WalEnumCreate, WalEnumAlterAdd, WalEnumAlterUpdate, WalVectorIndexCreate,
               WalVectorIndexDrop>
      data_ = WalTransactionEnd{};
};

constexpr bool IsWalDeltaDataImplicitTransactionEndVersion15(const WalDeltaData &delta) {
  return std::visit(utils::Overloaded{
                        // These delta actions are all found inside transactions so they don't
                        // indicate a transaction end.
                        [](WalVertexCreate const &) { return false; },
                        [](WalVertexDelete const &) { return false; },
                        [](WalVertexAddLabel const &) { return false; },
                        [](WalVertexRemoveLabel const &) { return false; },
                        [](WalVertexSetProperty const &) { return false; },
                        [](WalEdgeCreate const &) { return false; },
                        [](WalEdgeDelete const &) { return false; },
                        [](WalEdgeSetProperty const &) { return false; },

                        // This delta explicitly indicates that a transaction is done.
                        [](WalTransactionEnd const &) { return true; },

                        // These operations aren't transactional and they are encoded only using
                        // a single delta, so they each individually mark the end of their
                        // 'transaction'.
                        [](WalLabelIndexCreate const &) { return true; },
                        [](WalLabelIndexDrop const &) { return true; },
                        [](WalLabelIndexStatsSet const &) { return true; },
                        [](WalLabelIndexStatsClear const &) { return true; },
                        [](WalLabelPropertyIndexCreate const &) { return true; },
                        [](WalLabelPropertyIndexDrop const &) { return true; },
                        [](WalLabelPropertyIndexStatsSet const &) { return true; },
                        [](WalLabelPropertyIndexStatsClear const &) { return true; },
                        [](WalEdgeTypeIndexCreate const &) { return true; },
                        [](WalEdgeTypeIndexDrop const &) { return true; },
                        [](WalEdgeTypePropertyIndexCreate const &) { return true; },
                        [](WalEdgeTypePropertyIndexDrop const &) { return true; },
                        [](WalEdgePropertyIndexCreate const &) { return true; },
                        [](WalEdgePropertyIndexDrop const &) { return true; },
                        [](WalTextIndexCreate const &) { return true; },
                        [](WalTextIndexDrop const &) { return true; },
                        [](WalExistenceConstraintCreate const &) { return true; },
                        [](WalExistenceConstraintDrop const &) { return true; },
                        [](WalUniqueConstraintCreate const &) { return true; },
                        [](WalUniqueConstraintDrop const &) { return true; },
                        [](WalEnumCreate const &) { return true; },
                        [](WalEnumAlterAdd const &) { return true; },
                        [](WalEnumAlterUpdate const &) { return true; },
                        [](WalPointIndexCreate const &) { return true; },
                        [](WalPointIndexDrop const &) { return true; },
                        [](WalTypeConstraintCreate const &) { return true; },
                        [](WalTypeConstraintDrop const &) { return true; },
                        [](WalVectorIndexCreate const &) { return true; },
                        [](WalVectorIndexDrop const &) { return true; },
                    },
                    delta.data_);
}

constexpr bool IsWalDeltaDataTransactionEnd(const WalDeltaData &delta, const uint64_t version = kVersion) {
  if (version < kMetaDataDeltasHaveExplicitTransactionEnd) [[unlikely]] {
    return IsWalDeltaDataImplicitTransactionEndVersion15(delta);
  }
  // All deltas are now handled in a transactional scope, this is because it is
  // possible to have metadeltas and deltas in the same txn
  return std::holds_alternative<WalTransactionEnd>(delta.data_);
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
WalDeltaData ReadWalDeltaData(BaseDecoder *decoder, uint64_t version = kVersion);

/// Function used to skip the current WAL delta data. The function returns the
/// skipped delta type. The WAL delta header must be read before calling this
/// function.
/// @throw RecoveryFailure
bool SkipWalDeltaData(BaseDecoder *decoder, uint64_t version = kVersion);

/// Function used to encode a `Delta` that originated from a `Vertex`.
void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, SalientConfig::Items items, const Delta &delta,
                 const Vertex &vertex, uint64_t timestamp);

/// Function used to encode a `Delta` that originated from an `Edge`.
void EncodeDelta(BaseEncoder *encoder, NameIdMapper *name_id_mapper, const Delta &delta, const Edge &edge,
                 uint64_t timestamp);

/// Function used to encode the transaction end.
void EncodeTransactionEnd(BaseEncoder *encoder, uint64_t timestamp);

// Common to WAL & replication
void EncodeEdgeTypeIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, EdgeTypeId edge_type);
void EncodeEdgeTypePropertyIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, EdgeTypeId edge_type,
                                 PropertyId prop);
void EncodeEdgePropertyIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, PropertyId prop);
void EncodeEnumAlterAdd(BaseEncoder &encoder, EnumStore const &enum_store, Enum enum_val);
void EncodeEnumAlterUpdate(BaseEncoder &encoder, EnumStore const &enum_store, Enum enum_val,
                           std::string enum_value_old);
void EncodeEnumCreate(BaseEncoder &encoder, EnumStore const &enum_store, EnumTypeId etype);
void EncodeLabel(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label);
void EncodeLabelProperties(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                           std::span<PropertyPath const> properties);
void EncodeLabelProperties(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                           std::set<PropertyId> const &properties);
void EncodeTypeConstraint(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, PropertyId property,
                          TypeConstraintKind type);
void EncodeLabelProperty(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, PropertyId prop);
void EncodeLabelPropertyStats(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                              std::span<PropertyPath const> properties, LabelPropertyIndexStats const &stats);
void EncodeLabelStats(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label, LabelIndexStats stats);
void EncodeTextIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, std::string_view text_index_name,
                     LabelId label);
void EncodeVectorIndexSpec(BaseEncoder &encoder, NameIdMapper &name_id_mapper, const VectorIndexSpec &spec);
void EncodeVectorIndexName(BaseEncoder &encoder, std::string_view index_name);

void EncodeOperationPreamble(BaseEncoder &encoder, StorageMetadataOperation Op, uint64_t timestamp);

/// Function used to load the WAL data into the storage.
/// @throw RecoveryFailure
std::optional<RecoveryInfo> LoadWal(
    std::filesystem::path const &path, RecoveredIndicesAndConstraints *indices_constraints,
    std::optional<uint64_t> last_applied_delta_timestamp, utils::SkipList<Vertex> *vertices,
    utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
    SalientConfig::Items items, EnumStore *enum_store, SharedSchemaTracking *schema_info,
    std::function<std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>(Gid)> find_edge);

/// WalFile class used to append deltas and operations to the WAL file.
class WalFile {
 public:
  WalFile(const std::filesystem::path &wal_directory, utils::UUID const &uuid, const std::string_view epoch_id,
          SalientConfig::Items items, NameIdMapper *name_id_mapper, uint64_t seq_num,
          utils::FileRetainer *file_retainer);
  WalFile(std::filesystem::path current_wal_path, SalientConfig::Items items, NameIdMapper *name_id_mapper,
          uint64_t seq_num, uint64_t from_timestamp, uint64_t to_timestamp, uint64_t count,
          utils::FileRetainer *file_retainer);

  WalFile(const WalFile &) = delete;
  WalFile(WalFile &&) = delete;
  WalFile &operator=(const WalFile &) = delete;
  WalFile &operator=(WalFile &&) = delete;

  ~WalFile();

  void AppendDelta(const Delta &delta, const Vertex &vertex, uint64_t timestamp);
  void AppendDelta(const Delta &delta, const Edge &edge, uint64_t timestamp);

  void AppendTransactionEnd(uint64_t timestamp);

  void AppendOperation(StorageMetadataOperation operation, const std::optional<std::string> text_index_name,
                       LabelId label, const std::set<PropertyId> &properties, const LabelIndexStats &stats,
                       const LabelPropertyIndexStats &property_stats, uint64_t timestamp);

  void AppendOperation(StorageMetadataOperation operation, EdgeTypeId edge_type, const std::set<PropertyId> &properties,
                       uint64_t timestamp);

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

  auto encoder() -> BaseEncoder & { return wal_; }

  void UpdateStats(uint64_t timestamp);

 private:
  SalientConfig::Items items_;
  NameIdMapper *name_id_mapper_;
  Encoder<utils::OutputFile> wal_;
  std::filesystem::path path_;
  uint64_t from_timestamp_;
  uint64_t to_timestamp_;
  uint64_t count_;
  uint64_t seq_num_;

  utils::FileRetainer *file_retainer_;
};

}  // namespace memgraph::storage::durability
