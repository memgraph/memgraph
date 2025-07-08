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

#include "storage/v2/durability/wal.hpp"
#include <algorithm>
#include <cstdint>
#include <type_traits>
#include <usearch/index_plugins.hpp>

#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/marker.hpp"
#include "storage/v2/durability/metadata.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/schema_info.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"
#include "utils/tag.hpp"

namespace r = ranges;
namespace rv = r::views;

static constexpr std::string_view kInvalidWalErrorMessage =
    "Invalid WAL data! Your durability WAL files somehow got corrupted. Please contact the Memgraph team for support.";

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
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_CREATE);
    add_case(GLOBAL_EDGE_PROPERTY_INDEX_DROP);
    add_case(ENUM_ALTER_ADD);
    add_case(ENUM_ALTER_UPDATE);
    add_case(ENUM_CREATE);
    add_case(EXISTENCE_CONSTRAINT_CREATE);
    add_case(EXISTENCE_CONSTRAINT_DROP);
    add_case(LABEL_INDEX_CREATE);
    add_case(LABEL_INDEX_DROP);
    add_case(LABEL_INDEX_STATS_CLEAR);
    add_case(LABEL_INDEX_STATS_SET);
    add_case(LABEL_PROPERTIES_INDEX_CREATE);
    add_case(LABEL_PROPERTIES_INDEX_DROP);
    add_case(LABEL_PROPERTIES_INDEX_STATS_CLEAR);
    add_case(LABEL_PROPERTIES_INDEX_STATS_SET);
    add_case(TEXT_INDEX_CREATE);
    add_case(TEXT_INDEX_DROP);
    add_case(UNIQUE_CONSTRAINT_CREATE);
    add_case(UNIQUE_CONSTRAINT_DROP);
    add_case(TYPE_CONSTRAINT_CREATE);
    add_case(TYPE_CONSTRAINT_DROP);
    add_case(POINT_INDEX_CREATE);
    add_case(POINT_INDEX_DROP);
    add_case(VECTOR_INDEX_CREATE);
    add_case(VECTOR_EDGE_INDEX_CREATE);
    add_case(VECTOR_INDEX_DROP);
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

constexpr bool IsMarkerImplicitTransactionEndVersion15(Marker marker) {
  switch (marker) {
    using enum Marker;

    // These delta actions are all found inside transactions so they don't
    // indicate a transaction end.
    case DELTA_VERTEX_CREATE:
    case DELTA_VERTEX_DELETE:
    case DELTA_VERTEX_ADD_LABEL:
    case DELTA_VERTEX_REMOVE_LABEL:
    case DELTA_EDGE_CREATE:
    case DELTA_EDGE_DELETE:
    case DELTA_VERTEX_SET_PROPERTY:
    case DELTA_EDGE_SET_PROPERTY:
      return false;

    // This delta explicitly indicates that a transaction is done.
    // NOLINTNEXTLINE (bugprone-branch-clone)
    case DELTA_TRANSACTION_END:
      return true;

    // These operations aren't transactional and they are encoded only using
    // a single delta, so they each individually mark the end of their
    // 'transaction'.
    case DELTA_LABEL_INDEX_CREATE:
    case DELTA_LABEL_INDEX_DROP:
    case DELTA_LABEL_INDEX_STATS_SET:
    case DELTA_LABEL_INDEX_STATS_CLEAR:
    case DELTA_LABEL_PROPERTIES_INDEX_CREATE:
    case DELTA_LABEL_PROPERTIES_INDEX_DROP:
    case DELTA_LABEL_PROPERTIES_INDEX_STATS_SET:
    case DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR:
    case DELTA_EDGE_INDEX_CREATE:
    case DELTA_EDGE_INDEX_DROP:
    case DELTA_EDGE_PROPERTY_INDEX_CREATE:
    case DELTA_EDGE_PROPERTY_INDEX_DROP:
    case DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
    case DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP:
    case DELTA_TEXT_INDEX_CREATE:
    case DELTA_TEXT_INDEX_DROP:
    case DELTA_EXISTENCE_CONSTRAINT_CREATE:
    case DELTA_EXISTENCE_CONSTRAINT_DROP:
    case DELTA_UNIQUE_CONSTRAINT_CREATE:
    case DELTA_UNIQUE_CONSTRAINT_DROP:
    case DELTA_ENUM_CREATE:
    case DELTA_ENUM_ALTER_ADD:
    case DELTA_ENUM_ALTER_UPDATE:
    case DELTA_POINT_INDEX_CREATE:
    case DELTA_POINT_INDEX_DROP:
    case DELTA_TYPE_CONSTRAINT_CREATE:
    case DELTA_TYPE_CONSTRAINT_DROP:
    case DELTA_VECTOR_INDEX_CREATE:
    case DELTA_VECTOR_EDGE_INDEX_CREATE:
    case DELTA_VECTOR_INDEX_DROP:
      return true;

    // Not deltas
    case TYPE_NULL:
    case TYPE_BOOL:
    case TYPE_INT:
    case TYPE_DOUBLE:
    case TYPE_STRING:
    case TYPE_LIST:
    case TYPE_MAP:
    case TYPE_TEMPORAL_DATA:
    case TYPE_ZONED_TEMPORAL_DATA:
    case TYPE_PROPERTY_VALUE:
    case TYPE_ENUM:
    case TYPE_POINT_2D:
    case TYPE_POINT_3D:
    case SECTION_VERTEX:
    case SECTION_EDGE:
    case SECTION_MAPPER:
    case SECTION_METADATA:
    case SECTION_INDICES:
    case SECTION_CONSTRAINTS:
    case SECTION_DELTA:
    case SECTION_EPOCH_HISTORY:
    case SECTION_EDGE_INDICES:
    case SECTION_OFFSETS:
    case SECTION_ENUMS:
    case VALUE_FALSE:
    case VALUE_TRUE:
      throw RecoveryFailure(kInvalidWalErrorMessage);
  }
}

constexpr bool IsMarkerTransactionEnd(const Marker marker, const uint64_t version = kVersion) {
  if (version < kMetaDataDeltasHaveExplicitTransactionEnd) [[unlikely]] {
    return IsMarkerImplicitTransactionEndVersion15(marker);
  }
  // All deltas are now handled in a transactional scope
  return marker == Marker::DELTA_TRANSACTION_END;
}

// ========== concrete type decoders start here ==========
template <bool is_read>
auto Decode(utils::tag_type<Gid> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, Gid, void> {
  const auto gid = decoder->ReadUint();
  if (!gid) throw RecoveryFailure(kInvalidWalErrorMessage);
  if constexpr (is_read) {
    return Gid::FromUint(*gid);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<std::string> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, std::string, void> {
  if constexpr (is_read) {
    auto str = decoder->ReadString();
    if (!str) throw RecoveryFailure(kInvalidWalErrorMessage);
    return *std::move(str);
  } else {
    if (!decoder->SkipString()) throw RecoveryFailure(kInvalidWalErrorMessage);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<ExternalPropertyValue> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, ExternalPropertyValue, void> {
  if constexpr (is_read) {
    auto str = decoder->ReadExternalPropertyValue();
    if (!str) throw RecoveryFailure(kInvalidWalErrorMessage);
    return *std::move(str);
  } else {
    if (!decoder->SkipExternalPropertyValue()) throw RecoveryFailure(kInvalidWalErrorMessage);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<std::set<std::string, std::less<>>> /*unused*/, BaseDecoder *decoder,
            const uint64_t /*version*/) -> std::conditional_t<is_read, std::set<std::string, std::less<>>, void> {
  if constexpr (is_read) {
    std::set<std::string, std::less<>> strings;
    const auto count = decoder->ReadUint();
    if (!count) throw RecoveryFailure(kInvalidWalErrorMessage);
    for (uint64_t i = 0; i < *count; ++i) {
      auto str = decoder->ReadString();
      if (!str) throw RecoveryFailure(kInvalidWalErrorMessage);
      strings.emplace(*std::move(str));
    }
    return strings;
  } else {
    const auto count = decoder->ReadUint();
    if (!count) throw RecoveryFailure(kInvalidWalErrorMessage);
    for (uint64_t i = 0; i < *count; ++i) {
      if (!decoder->SkipString()) throw RecoveryFailure(kInvalidWalErrorMessage);
    }
  }
}

template <bool is_read, typename T>
auto Decode(utils::tag_type<std::vector<T>> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, std::vector<T>, void> {
  if constexpr (is_read) {
    const auto count = decoder->ReadUint();
    if (!count) throw RecoveryFailure(kInvalidWalErrorMessage);
    std::vector<T> values;
    values.reserve(*count);
    for (uint64_t i = 0; i < *count; ++i) {
      auto value = Decode<true>(utils::tag_t<T>, decoder, 0);
      values.emplace_back(std::move(value));
    }
    return values;
  } else {
    const auto count = decoder->ReadUint();
    if (!count) throw RecoveryFailure(kInvalidWalErrorMessage);
    for (uint64_t i = 0; i < *count; ++i) {
      Decode<false>(utils::tag_t<T>, decoder, 0);
    }
  }
}

template <bool is_read>
auto Decode(utils::tag_type<TypeConstraintKind> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, TypeConstraintKind, void> {
  if constexpr (is_read) {
    auto kind = decoder->ReadUint();
    if (!kind) throw RecoveryFailure(kInvalidWalErrorMessage);
    return static_cast<TypeConstraintKind>(*kind);
  } else {
    if (!decoder->ReadUint()) throw RecoveryFailure(kInvalidWalErrorMessage);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<uint16_t> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, uint16_t, void> {
  const auto uint16 = decoder->ReadUint();
  if (!uint16) throw RecoveryFailure(kInvalidWalErrorMessage);
  if constexpr (is_read) {
    return static_cast<uint16_t>(*uint16);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<uint8_t> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, uint8_t, void> {
  const auto uint8 = decoder->ReadUint();
  if (!uint8) throw RecoveryFailure(kInvalidWalErrorMessage);
  if constexpr (is_read) {
    return static_cast<uint8_t>(*uint8);
  }
}

template <bool is_read>
auto Decode(utils::tag_type<std::size_t> /*unused*/, BaseDecoder *decoder, const uint64_t /*version*/)
    -> std::conditional_t<is_read, std::size_t, void> {
  const auto size = decoder->ReadUint();
  if (!size) throw RecoveryFailure(kInvalidWalErrorMessage);
  if constexpr (is_read) {
    return static_cast<std::size_t>(*size);
  }
}

// ========== concrete type decoders end here ==========

template <typename T>
auto Read(BaseDecoder *decoder, const uint64_t version) -> T;
template <typename T>
auto Skip(BaseDecoder *decoder, const uint64_t version) -> void;

template <typename T>
concept IsReadSkip = requires {
  typename T::ctr_types;
};

// Generic helper decoder, please keep after the concrete type decoders
template <bool is_read, IsReadSkip T>
auto Decode(utils::tag_type<T> /*unused*/, BaseDecoder *decoder, const uint64_t version)
    -> std::conditional_t<is_read, T, void> {
  if constexpr (is_read) {
    return Read<T>(decoder, version);
  } else {
    Skip<T>(decoder, version);
  }
}

// Generic helper decoder, please keep after the concrete type decoders
template <bool is_read, auto MIN_VER, typename Type>
auto Decode(utils::tag_type<VersionDependant<MIN_VER, Type>> /*unused*/, BaseDecoder *decoder, const uint64_t version)
    -> std::conditional_t<is_read, std::optional<Type>, void> {
  if (MIN_VER <= version) {
    return Decode<is_read>(utils::tag_t<Type>, decoder, version);
  }
  if constexpr (is_read) {
    return std::nullopt;
  }
}

// Generic helper decoder, please keep after the concrete type decoders
template <bool is_read, auto MIN_VER, typename Before, typename After, auto Upgrader>
auto Decode(utils::tag_type<VersionDependantUpgradable<MIN_VER, Before, After, Upgrader>> /*unused*/,
            BaseDecoder *decoder, const uint64_t version) -> std::conditional_t<is_read, After, void> {
  if (MIN_VER <= version) {
    return Decode<is_read>(utils::tag_t<After>, decoder, version);
  }
  if constexpr (is_read) {
    return Upgrader(Decode<true>(utils::tag_t<Before>, decoder, version));
  } else {
    Decode<false>(utils::tag_t<Before>, decoder, version);
  }
}

template <typename T>
auto Read(BaseDecoder *decoder, const uint64_t version) -> T {
  using ctr_types = typename T::ctr_types;

  return [&]<auto... I>(std::index_sequence<I...>) {
    // https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2023/n4950.pdf
    // see [dcl.init.list] 9.4.5.4
    // Ordering of these constructor argument calls is well defined
    return T{Decode<true>(utils::tag_t<std::tuple_element_t<I, ctr_types>>, decoder, version)...};
  }
  (std::make_index_sequence<std::tuple_size_v<ctr_types>>{});
}

template <typename T>
auto Skip(BaseDecoder *decoder, const uint64_t version) -> void {
  using ctr_types = typename T::ctr_types;

  [&]<auto... I>(std::index_sequence<I...>) {
    (Decode<false>(utils::tag_t<std::tuple_element_t<I, ctr_types>>, decoder, version), ...);
  }
  (std::make_index_sequence<std::tuple_size_v<ctr_types>>{});
}

// Function used to either read or skip the current WAL delta data. The WAL
// delta header must be read before calling this function. If the delta data is
// read then the data returned is valid, if the delta data is skipped then the
// returned data is not guaranteed to be set (it could be empty) and shouldn't
// be used.
// @throw RecoveryFailure
template <bool read_data>
auto ReadSkipWalDeltaData(BaseDecoder *decoder, const uint64_t version)
    -> std::conditional_t<read_data, WalDeltaData, bool> {
  auto action = decoder->ReadMarker();
  if (!action) throw RecoveryFailure(kInvalidWalErrorMessage);

    // NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define read_skip(enum_val, decode_to)                     \
  case Marker::DELTA_##enum_val: {                         \
    if constexpr (read_data) {                             \
      return {.data_ = Read<decode_to>(decoder, version)}; \
    } else {                                               \
      Skip<decode_to>(decoder, version);                   \
      return IsMarkerTransactionEnd(*action, version);     \
    }                                                      \
  }

  switch (*action) {
    read_skip(VERTEX_CREATE, WalVertexCreate);
    read_skip(VERTEX_DELETE, WalVertexDelete);
    read_skip(VERTEX_ADD_LABEL, WalVertexAddLabel);
    read_skip(VERTEX_REMOVE_LABEL, WalVertexRemoveLabel);
    read_skip(VERTEX_SET_PROPERTY, WalVertexSetProperty);
    read_skip(EDGE_SET_PROPERTY, WalEdgeSetProperty);
    read_skip(EDGE_CREATE, WalEdgeCreate);
    read_skip(EDGE_DELETE, WalEdgeDelete);
    read_skip(TRANSACTION_END, WalTransactionEnd);
    read_skip(LABEL_INDEX_CREATE, WalLabelIndexCreate);
    read_skip(LABEL_INDEX_DROP, WalLabelIndexDrop);
    read_skip(LABEL_INDEX_STATS_CLEAR, WalLabelIndexStatsClear);
    read_skip(LABEL_PROPERTIES_INDEX_STATS_CLEAR, WalLabelPropertyIndexStatsClear);
    read_skip(EDGE_INDEX_CREATE, WalEdgeTypeIndexCreate);
    read_skip(EDGE_INDEX_DROP, WalEdgeTypeIndexDrop);
    read_skip(LABEL_INDEX_STATS_SET, WalLabelIndexStatsSet);
    read_skip(LABEL_PROPERTIES_INDEX_CREATE, WalLabelPropertyIndexCreate);
    read_skip(LABEL_PROPERTIES_INDEX_DROP, WalLabelPropertyIndexDrop);
    read_skip(POINT_INDEX_CREATE, WalPointIndexCreate);
    read_skip(POINT_INDEX_DROP, WalPointIndexDrop);
    read_skip(EXISTENCE_CONSTRAINT_CREATE, WalExistenceConstraintCreate);
    read_skip(EXISTENCE_CONSTRAINT_DROP, WalExistenceConstraintDrop);
    read_skip(LABEL_PROPERTIES_INDEX_STATS_SET, WalLabelPropertyIndexStatsSet);
    read_skip(EDGE_PROPERTY_INDEX_CREATE, WalEdgeTypePropertyIndexCreate);
    read_skip(EDGE_PROPERTY_INDEX_DROP, WalEdgeTypePropertyIndexDrop);
    read_skip(GLOBAL_EDGE_PROPERTY_INDEX_CREATE, WalEdgePropertyIndexCreate);
    read_skip(GLOBAL_EDGE_PROPERTY_INDEX_DROP, WalEdgePropertyIndexDrop);
    read_skip(UNIQUE_CONSTRAINT_CREATE, WalUniqueConstraintCreate);
    read_skip(UNIQUE_CONSTRAINT_DROP, WalUniqueConstraintDrop);
    read_skip(TYPE_CONSTRAINT_CREATE, WalTypeConstraintCreate);
    read_skip(TYPE_CONSTRAINT_DROP, WalTypeConstraintDrop);
    read_skip(TEXT_INDEX_CREATE, WalTextIndexCreate);
    read_skip(TEXT_INDEX_DROP, WalTextIndexDrop);
    read_skip(ENUM_CREATE, WalEnumCreate);
    read_skip(ENUM_ALTER_ADD, WalEnumAlterAdd);
    read_skip(ENUM_ALTER_UPDATE, WalEnumAlterUpdate);
    read_skip(VECTOR_INDEX_CREATE, WalVectorIndexCreate);
    read_skip(VECTOR_EDGE_INDEX_CREATE, WalVectorEdgeIndexCreate);
    read_skip(VECTOR_INDEX_DROP, WalVectorIndexDrop);

    // Other markers are not actions
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
      throw RecoveryFailure(kInvalidWalErrorMessage);
  }
#undef read_skip
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
    if (!marker || *marker != Marker::SECTION_OFFSETS) throw RecoveryFailure(kInvalidWalErrorMessage);

    auto wal_size = wal.GetSize();
    if (!wal_size) throw RecoveryFailure(kInvalidWalErrorMessage);

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
    if (!marker || *marker != Marker::SECTION_METADATA) throw RecoveryFailure(kInvalidWalErrorMessage);

    auto maybe_uuid = wal.ReadString();
    if (!maybe_uuid) throw RecoveryFailure(kInvalidWalErrorMessage);
    info.uuid = std::move(*maybe_uuid);

    auto maybe_epoch_id = wal.ReadString();
    if (!maybe_epoch_id) throw RecoveryFailure(kInvalidWalErrorMessage);
    info.epoch_id = std::move(*maybe_epoch_id);

    auto maybe_seq_num = wal.ReadUint();
    if (!maybe_seq_num) throw RecoveryFailure(kInvalidWalErrorMessage);
    info.seq_num = *maybe_seq_num;
  }

  // Read deltas.
  info.num_deltas = 0;
  auto validate_delta = [&wal, version = *version]() -> std::optional<std::pair<uint64_t, bool>> {
    try {
      auto timestamp = ReadWalDeltaHeader(&wal);
      auto is_transaction_end = SkipWalDeltaData(&wal, version);
      return {{timestamp, is_transaction_end}};
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

  if (info.num_deltas == 0) throw RecoveryFailure(kInvalidWalErrorMessage);

  return info;
}

// Function used to read the WAL delta header. The function returns the delta
// timestamp.
uint64_t ReadWalDeltaHeader(BaseDecoder *decoder) {
  auto marker = decoder->ReadMarker();
  if (!marker || *marker != Marker::SECTION_DELTA) throw RecoveryFailure(kInvalidWalErrorMessage);

  auto timestamp = decoder->ReadUint();
  if (!timestamp) throw RecoveryFailure(kInvalidWalErrorMessage);
  return *timestamp;
}

// Function used to read the current WAL delta data. The WAL delta header must
// be read before calling this function.
WalDeltaData ReadWalDeltaData(BaseDecoder *decoder, const uint64_t version) {
  return ReadSkipWalDeltaData<true>(decoder, version);
}

// Function used to skip the current WAL delta data. The WAL delta header must
// be read before calling this function.
bool SkipWalDeltaData(BaseDecoder *decoder, const uint64_t version) {
  return ReadSkipWalDeltaData<false>(decoder, version);
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
      encoder->WriteExternalPropertyValue(
          ToExternalPropertyValue(vertex.properties.GetProperty(delta.property.key), name_id_mapper));
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
      encoder->WriteExternalPropertyValue(
          ToExternalPropertyValue(edge.properties.GetProperty(delta.property.key), name_id_mapper));
      DMG_ASSERT(delta.property.out_vertex, "Out vertex undefined!");
      encoder->WriteUint(delta.property.out_vertex->gid.AsUint());
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

std::optional<RecoveryInfo> LoadWal(
    const std::filesystem::path &path, RecoveredIndicesAndConstraints *indices_constraints,
    const std::optional<uint64_t> last_applied_delta_timestamp, utils::SkipList<Vertex> *vertices,
    utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
    SalientConfig::Items items, EnumStore *enum_store, SharedSchemaTracking *schema_info,
    std::function<std::optional<std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *>>(Gid)> find_edge) {
  spdlog::info("Trying to load WAL file {}.", path);

  Decoder wal;
  auto version = wal.Initialize(path, kWalMagic);
  if (!version) throw RecoveryFailure("Couldn't read WAL magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure("Invalid WAL version!");

  // Read wal info.
  auto info = ReadWalInfo(path);

  // Check timestamp.
  if (last_applied_delta_timestamp && info.to_timestamp <= *last_applied_delta_timestamp) {
    spdlog::info("Skip loading WAL file because it is too old. {} <= {}", info.to_timestamp,
                 *last_applied_delta_timestamp);
    return std::nullopt;
  }

  RecoveryInfo ret;
  ret.last_durable_timestamp = info.to_timestamp;

  // Recover deltas
  wal.SetPosition(info.offset_deltas);
  uint64_t deltas_applied = 0;
  auto edge_acc = edges->access();
  auto vertex_acc = vertices->access();
  spdlog::info("WAL file contains {} deltas.", info.num_deltas);

  auto delta_apply = utils::Overloaded{
      [&](WalVertexCreate const &data) {
        auto [vertex, inserted] = vertex_acc.insert(Vertex{data.gid, nullptr});
        if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");
        ret.next_vertex_id = std::max(ret.next_vertex_id, data.gid.AsUint() + 1);
        if (schema_info) schema_info->AddVertex(&*vertex);
      },
      [&](WalVertexDelete const &data) {
        const auto vertex = vertex_acc.find(data.gid);
        if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");
        if (!vertex->in_edges.empty() || !vertex->out_edges.empty())
          throw RecoveryFailure("The vertex can't be deleted because it still has edges!");
        if (!vertex_acc.remove(data.gid)) throw RecoveryFailure("The vertex must be removed here!");
        if (schema_info) schema_info->DeleteVertex(&*vertex);
      },
      [&](WalVertexAddLabel const &data) {
        const auto vertex = vertex_acc.find(data.gid);
        if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");
        const auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        if (r::contains(vertex->labels, label_id)) throw RecoveryFailure("The vertex already has the label!");
        std::optional<utils::small_vector<LabelId>> old_labels{};
        if (schema_info) old_labels.emplace(vertex->labels);
        vertex->labels.push_back(label_id);
        if (schema_info) schema_info->UpdateLabels(&*vertex, *old_labels, vertex->labels, items.properties_on_edges);
      },
      [&](WalVertexRemoveLabel const &data) {
        const auto vertex = vertex_acc.find(data.gid);
        if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");
        const auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto it = r::find(vertex->labels, label_id);
        if (it == vertex->labels.end()) throw RecoveryFailure("The vertex doesn't have the label!");
        std::optional<utils::small_vector<LabelId>> old_labels{};
        if (schema_info) old_labels.emplace(vertex->labels);
        std::swap(*it, vertex->labels.back());
        vertex->labels.pop_back();
        if (schema_info) schema_info->UpdateLabels(&*vertex, *old_labels, vertex->labels, items.properties_on_edges);
      },
      [&](WalVertexSetProperty const &data) {
        const auto vertex = vertex_acc.find(data.gid);
        if (vertex == vertex_acc.end()) throw RecoveryFailure("The vertex doesn't exist!");
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        const auto property_value = ToPropertyValue(data.value, name_id_mapper);
        if (schema_info) {
          const auto old_type = vertex->properties.GetExtendedPropertyType(property_id);
          schema_info->SetProperty(&*vertex, property_id, ExtendedPropertyType{(property_value)}, old_type);
        }
        vertex->properties.SetProperty(property_id, property_value);
      },
      [&](WalEdgeCreate const &data) {
        const auto from_vertex = vertex_acc.find(data.from_vertex);
        if (from_vertex == vertex_acc.end()) throw RecoveryFailure("The from vertex doesn't exist!");
        const auto to_vertex = vertex_acc.find(data.to_vertex);
        if (to_vertex == vertex_acc.end()) throw RecoveryFailure("The to vertex doesn't exist!");

        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        auto edge_ref = std::invoke([&]() -> EdgeRef {
          if (items.properties_on_edges) {
            auto [edge, inserted] = edge_acc.insert(Edge{(data.gid), nullptr});
            if (!inserted) throw RecoveryFailure("The edge must be inserted here!");
            return EdgeRef{&*edge};
          }
          return EdgeRef{data.gid};
        });
        auto out_link = std::tuple{edge_type_id, &*to_vertex, edge_ref};
        if (r::contains(from_vertex->out_edges, out_link))
          throw RecoveryFailure("The from vertex already has this edge!");
        from_vertex->out_edges.push_back(out_link);
        auto in_link = std::tuple{edge_type_id, &*from_vertex, edge_ref};
        if (r::contains(to_vertex->in_edges, in_link)) throw RecoveryFailure("The to vertex already has this edge!");
        to_vertex->in_edges.push_back(in_link);

        ret.next_edge_id = std::max(ret.next_edge_id, data.gid.AsUint() + 1);

        // Increment edge count.
        edge_count->fetch_add(1, std::memory_order_acq_rel);

        if (schema_info) schema_info->CreateEdge(&*from_vertex, &*to_vertex, edge_type_id);
      },
      [&](WalEdgeDelete const &data) {
        const auto from_vertex = vertex_acc.find(data.from_vertex);
        if (from_vertex == vertex_acc.end()) throw RecoveryFailure("The from vertex doesn't exist!");
        const auto to_vertex = vertex_acc.find(data.to_vertex);
        if (to_vertex == vertex_acc.end()) throw RecoveryFailure("The to vertex doesn't exist!");

        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        auto edge_ref = std::invoke([&]() -> EdgeRef {
          if (items.properties_on_edges) {
            auto edge = edge_acc.find(data.gid);
            if (edge == edge_acc.end()) throw RecoveryFailure("The edge doesn't exist!");
            return EdgeRef{&*edge};
          }
          return EdgeRef{data.gid};
        });

        {
          auto out_link = std::tuple{edge_type_id, &*to_vertex, edge_ref};
          auto it = r::find(from_vertex->out_edges, out_link);
          if (it == from_vertex->out_edges.end()) throw RecoveryFailure("The from vertex doesn't have this edge!");
          std::swap(*it, from_vertex->out_edges.back());
          from_vertex->out_edges.pop_back();
        }
        {
          auto in_link = std::tuple{edge_type_id, &*from_vertex, edge_ref};
          auto it = r::find(to_vertex->in_edges, in_link);
          if (it == to_vertex->in_edges.end()) throw RecoveryFailure("The to vertex doesn't have this edge!");
          std::swap(*it, to_vertex->in_edges.back());
          to_vertex->in_edges.pop_back();
        }
        if (items.properties_on_edges) {
          if (!edge_acc.remove(data.gid)) throw RecoveryFailure("The edge must be removed here!");
        }

        // Decrement edge count.
        edge_count->fetch_add(-1, std::memory_order_acq_rel);

        if (schema_info)
          schema_info->DeleteEdge(edge_type_id, edge_ref, &*from_vertex, &*to_vertex, items.properties_on_edges);
      },
      [&](WalEdgeSetProperty const &data) {
        if (!items.properties_on_edges)
          throw RecoveryFailure(
              "The WAL has properties on edges, but the storage is "
              "configured without properties on edges!");

        auto edge = edge_acc.find(data.gid);
        if (edge == edge_acc.end()) throw RecoveryFailure("The edge doesn't exist!");
        const auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        const auto property_value = ToPropertyValue(data.value, name_id_mapper);

        if (schema_info) {
          const auto &[edge_ref, edge_type, from_vertex, to_vertex] = std::invoke([&] {
            if (data.from_gid.has_value()) {
              const auto from_vertex = vertex_acc.find(data.from_gid);
              if (from_vertex == vertex_acc.end()) throw RecoveryFailure("The from vertex doesn't exist!");
              const auto found_edge = r::find_if(from_vertex->out_edges, [&edge](const auto &edge_info) {
                const auto &[edge_type, to_vertex, edge_ref] = edge_info;
                return edge_ref.ptr == &*edge;
              });
              if (found_edge == from_vertex->out_edges.end()) throw RecoveryFailure("Recovery failed, edge not found.");
              const auto &[edge_type, to_vertex, edge_ref] = *found_edge;
              return std::tuple{edge_ref, edge_type, &*from_vertex, to_vertex};
            }
            // Fallback on user defined find edge function
            const auto maybe_edge = find_edge(edge->gid);
            if (!maybe_edge) throw RecoveryFailure("Recovery failed, edge not found.");
            return *maybe_edge;
          });

          const auto old_type = edge->properties.GetExtendedPropertyType(property_id);
          schema_info->SetProperty(edge_type, from_vertex, to_vertex, property_id, ExtendedPropertyType{property_value},
                                   old_type, items.properties_on_edges);
        }

        edge->properties.SetProperty(property_id, property_value);
      },
      [&](WalTransactionEnd const &) { /*Nothing to apply*/ },
      [&](WalLabelIndexCreate const &data) {
        const auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        AddRecoveredIndexConstraint(&indices_constraints->indices.label, label_id, "The label index already exists!");
      },
      [&](WalLabelIndexDrop const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.label, label_id, "The label index doesn't exist!");
      },
      [&](WalEdgeTypeIndexCreate const &data) {
        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        AddRecoveredIndexConstraint(&indices_constraints->indices.edge, edge_type_id,
                                    "The edge-type index already exists!");
      },
      [&](WalEdgeTypeIndexDrop const &data) {
        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.edge, edge_type_id,
                                       "The edge-type index doesn't exist!");
      },
      [&](WalEdgeTypePropertyIndexCreate const &data) {
        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        AddRecoveredIndexConstraint(&indices_constraints->indices.edge_type_property, {edge_type_id, property_id},
                                    "The edge-type + property index already exists!");
      },
      [&](WalEdgeTypePropertyIndexDrop const &data) {
        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.edge_type_property, {edge_type_id, property_id},
                                       "The edge-type + property index doesn't exist!");
      },
      [&](WalEdgePropertyIndexCreate const &data) {
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        AddRecoveredIndexConstraint(&indices_constraints->indices.edge_property, {property_id},
                                    "The global edge property index already exists!");
      },
      [&](WalEdgePropertyIndexDrop const &data) {
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.edge_property, {property_id},
                                       "The global edge property index doesn't exist!");
      },
      [&](WalLabelIndexStatsSet const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        LabelIndexStats stats{};
        if (!FromJson(data.json_stats, stats)) {
          throw RecoveryFailure("Failed to read statistics!");
        }
        indices_constraints->indices.label_stats.emplace_back(label_id, stats);
      },
      [&](WalLabelIndexStatsClear const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        RemoveRecoveredIndexStats(&indices_constraints->indices.label_stats, label_id,
                                  "The label index stats doesn't exist!");
      },
      [&](WalLabelPropertyIndexCreate const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto prop_ids = data.composite_property_paths.convert(name_id_mapper);
        AddRecoveredIndexConstraint(&indices_constraints->indices.label_properties, {label_id, std::move(prop_ids)},
                                    "The label property index already exists!");
      },
      [&](WalLabelPropertyIndexDrop const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto prop_ids = data.composite_property_paths.convert(name_id_mapper);
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.label_properties, {label_id, std::move(prop_ids)},
                                       "The label property index doesn't exist!");
      },
      [&](WalPointIndexCreate const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        AddRecoveredIndexConstraint(&indices_constraints->indices.point_label_property, {label_id, property_id},
                                    "The label property index already exists!");
      },
      [&](WalPointIndexDrop const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.point_label_property, {label_id, property_id},
                                       "The label property index doesn't exist!");
      },
      [&](WalLabelPropertyIndexStatsSet const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto prop_ids = data.composite_property_paths.convert(name_id_mapper);
        LabelPropertyIndexStats stats{};
        if (!FromJson(data.json_stats, stats)) {
          throw RecoveryFailure("Failed to read statistics!");
        }
        indices_constraints->indices.label_property_stats.emplace_back(label_id,
                                                                       std::make_pair(std::move(prop_ids), stats));
      },
      [&](WalLabelPropertyIndexStatsClear const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        RemoveRecoveredIndexStats(&indices_constraints->indices.label_property_stats, label_id,
                                  "The label index stats doesn't exist!");
      },
      [&](WalTextIndexCreate const &data) {
        auto label = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        AddRecoveredIndexConstraint(&indices_constraints->indices.text_indices, {data.index_name, label},
                                    "The text index already exists!");
      },
      [&](WalTextIndexDrop const &data) {
        auto label = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        RemoveRecoveredIndexConstraint(&indices_constraints->indices.text_indices, {data.index_name, label},
                                       "The text index doesn't exist!");
      },
      [&](WalExistenceConstraintCreate const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        AddRecoveredIndexConstraint(&indices_constraints->constraints.existence, {label_id, property_id},
                                    "The existence constraint already exists!");
      },
      [&](WalExistenceConstraintDrop const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        RemoveRecoveredIndexConstraint(&indices_constraints->constraints.existence, {label_id, property_id},
                                       "The existence constraint doesn't exist!");
      },
      [&](WalUniqueConstraintCreate const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        std::set<PropertyId> property_ids;
        for (const auto &prop : data.properties) {
          property_ids.insert(PropertyId::FromUint(name_id_mapper->NameToId(prop)));
        }
        AddRecoveredIndexConstraint(&indices_constraints->constraints.unique, {label_id, property_ids},
                                    "The unique constraint already exists!");
      },
      [&](WalUniqueConstraintDrop const &data) {
        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        std::set<PropertyId> property_ids;
        for (const auto &prop : data.properties) {
          property_ids.insert(PropertyId::FromUint(name_id_mapper->NameToId(prop)));
        }
        RemoveRecoveredIndexConstraint(&indices_constraints->constraints.unique, {label_id, property_ids},
                                       "The unique constraint doesn't exist!");
      },
      [&](WalTypeConstraintCreate const &data) {
        auto label = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        AddRecoveredIndexConstraint(&indices_constraints->constraints.type, {label, property, data.kind},
                                    "The type constraint already exists!");
      },
      [&](WalTypeConstraintDrop const &data) {
        auto label = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        RemoveRecoveredIndexConstraint(&indices_constraints->constraints.type, {label, property, data.kind},
                                       "The type constraint doesn't exist!");
      },
      [&](WalEnumCreate &data) {
        auto res = enum_store->RegisterEnum(std::move(data.etype), std::move(data.evalues));
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
      },
      [&](WalEnumAlterAdd &data) {
        auto res = enum_store->AddValue(std::move(data.etype), std::move(data.evalue));
        if (res.HasError()) {
          switch (res.GetError()) {
            case EnumStorageError::InvalidValue:
              throw RecoveryFailure("Enum value already exists.");
            case EnumStorageError::UnknownEnumType:
              throw RecoveryFailure("Unknown Enum type.");
            default:
              // Should not happen
              throw RecoveryFailure("Enum could not be altered.");
          }
        }
      },
      [&](WalEnumAlterUpdate const &data) {
        auto const &[enum_name, enum_value_old, enum_value_new] = data;
        auto res = enum_store->UpdateValue(enum_name, enum_value_old, enum_value_new);
        if (res.HasError()) {
          switch (res.GetError()) {
            case EnumStorageError::InvalidValue:
              throw RecoveryFailure("Enum value {}::{} already exists.", enum_name, enum_value_new);
            case EnumStorageError::UnknownEnumType:
              throw RecoveryFailure("Unknown Enum name {}.", enum_name);
            case EnumStorageError::UnknownEnumValue:
              throw RecoveryFailure("Unknown Enum value {}::{}.", enum_name, enum_value_old);
            default:
              // Should not happen
              throw RecoveryFailure("Enum could not be altered.");
          }
        }
      },
      [&](WalVectorIndexCreate const &data) {
        if (r::any_of(indices_constraints->indices.vector_indices,
                      [&](const auto &index) { return index.index_name == data.index_name; }) ||
            r::any_of(indices_constraints->indices.vector_edge_indices,
                      [&](const auto &index) { return index.index_name == data.index_name; })) {
          throw RecoveryFailure("The vector index already exists!");
        }

        auto label_id = LabelId::FromUint(name_id_mapper->NameToId(data.label));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        const auto unum_metric_kind = MetricFromName(data.metric_kind);
        auto scalar_kind = data.scalar_kind ? static_cast<unum::usearch::scalar_kind_t>(*data.scalar_kind)
                                            : unum::usearch::scalar_kind_t::f32_k;
        indices_constraints->indices.vector_indices.emplace_back(data.index_name, label_id, property_id,
                                                                 unum_metric_kind, data.dimension,
                                                                 data.resize_coefficient, data.capacity, scalar_kind);
      },
      [&](WalVectorEdgeIndexCreate const &data) {
        if (r::any_of(indices_constraints->indices.vector_indices,
                      [&](const auto &index) { return index.index_name == data.index_name; }) ||
            r::any_of(indices_constraints->indices.vector_edge_indices,
                      [&](const auto &index) { return index.index_name == data.index_name; })) {
          throw RecoveryFailure("The vector edge index already exists!");
        }

        auto edge_type_id = EdgeTypeId::FromUint(name_id_mapper->NameToId(data.edge_type));
        auto property_id = PropertyId::FromUint(name_id_mapper->NameToId(data.property));
        const auto unum_metric_kind = MetricFromName(data.metric_kind);
        auto scalar_kind = static_cast<unum::usearch::scalar_kind_t>(data.scalar_kind);
        indices_constraints->indices.vector_edge_indices.emplace_back(
            data.index_name, edge_type_id, property_id, unum_metric_kind, data.dimension, data.resize_coefficient,
            data.capacity, scalar_kind);
      },
      [&](WalVectorIndexDrop const &data) {
        std::erase_if(indices_constraints->indices.vector_indices,
                      [&](const auto &index) { return index.index_name == data.index_name; });
      },
  };

  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    // Read WAL delta header to find out the delta timestamp.
    auto timestamp = ReadWalDeltaHeader(&wal);

    if (!last_applied_delta_timestamp || timestamp > *last_applied_delta_timestamp) {
      // This delta should be loaded.
      auto delta = ReadWalDeltaData(&wal, *version);
      std::visit(delta_apply, delta.data_);
      ++deltas_applied;
      ret.next_timestamp = std::max(ret.next_timestamp, timestamp + 1);
    } else {
      // This delta should be skipped.
      SkipWalDeltaData(&wal, *version);
    }
  }

  spdlog::info("Applied {} deltas from WAL. Skipped {} deltas, because they were too old.", deltas_applied,
               info.num_deltas - deltas_applied);

  return ret;
}

WalFile::WalFile(const std::filesystem::path &wal_directory, utils::UUID const &uuid, const std::string_view epoch_id,
                 SalientConfig::Items items, NameIdMapper *name_id_mapper, uint64_t seq_num,
                 utils::FileRetainer *file_retainer)
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
  wal_.WriteString(std::string{uuid});
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

void EncodeLabelPropertyStats(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                              std::span<PropertyPath const> properties, LabelPropertyIndexStats const &stats) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteUint(properties.size());
  for (auto const &path : properties) {
    encoder.WriteUint(path.size());
    for (auto const &segment : path) {
      encoder.WriteString(name_id_mapper.IdToName(segment.AsUint()));
    }
  }
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

void EncodeEdgePropertyIndex(BaseEncoder &encoder, NameIdMapper &name_id_mapper, PropertyId prop) {
  encoder.WriteString(name_id_mapper.IdToName(prop.AsUint()));
}

void EncodeLabelProperties(BaseEncoder &encoder, NameIdMapper &name_id_mapper, LabelId label,
                           std::span<PropertyPath const> properties) {
  encoder.WriteString(name_id_mapper.IdToName(label.AsUint()));
  encoder.WriteUint(properties.size());
  for (const auto &path : properties) {
    encoder.WriteUint(path.size());
    for (const auto &property : path) {
      encoder.WriteString(name_id_mapper.IdToName(property.AsUint()));
    }
  }
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

void EncodeVectorIndexSpec(BaseEncoder &encoder, NameIdMapper &name_id_mapper, const VectorIndexSpec &index_spec) {
  encoder.WriteString(index_spec.index_name);
  encoder.WriteString(name_id_mapper.IdToName(index_spec.label_id.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(index_spec.property.AsUint()));
  encoder.WriteString(NameFromMetric(index_spec.metric_kind));
  encoder.WriteUint(index_spec.dimension);
  encoder.WriteUint(index_spec.resize_coefficient);
  encoder.WriteUint(index_spec.capacity);
  encoder.WriteUint(static_cast<uint64_t>(index_spec.scalar_kind));
}

void EncodeVectorEdgeIndexSpec(BaseEncoder &encoder, NameIdMapper &name_id_mapper,
                               const VectorEdgeIndexSpec &index_spec) {
  encoder.WriteString(index_spec.index_name);
  encoder.WriteString(name_id_mapper.IdToName(index_spec.edge_type_id.AsUint()));
  encoder.WriteString(name_id_mapper.IdToName(index_spec.property.AsUint()));
  encoder.WriteString(NameFromMetric(index_spec.metric_kind));
  encoder.WriteUint(index_spec.dimension);
  encoder.WriteUint(index_spec.resize_coefficient);
  encoder.WriteUint(index_spec.capacity);
  encoder.WriteUint(static_cast<uint64_t>(index_spec.scalar_kind));
}

void EncodeVectorIndexName(BaseEncoder &encoder, std::string_view index_name) { encoder.WriteString(index_name); }

void EncodeOperationPreamble(BaseEncoder &encoder, StorageMetadataOperation Op, uint64_t timestamp) {
  encoder.WriteMarker(Marker::SECTION_DELTA);
  encoder.WriteUint(timestamp);
  encoder.WriteMarker(OperationToMarker(Op));
}

auto UpgradeForNestedIndices(CompositeStr v) -> std::vector<PathStr> {
  auto wrap_singular_path = [](auto &&path) -> PathStr { return std::vector{std::forward<decltype(path)>(path)}; };
  return v | ranges::views::transform(wrap_singular_path) | ranges::to_vector;
};

auto CompositePropertyPaths::convert(memgraph::storage::NameIdMapper *mapper) const
    -> std::vector<memgraph::storage::PropertyPath> {
  auto to_propertyid = [&](std::string_view prop_name) -> PropertyId {
    return PropertyId::FromUint(mapper->NameToId(prop_name));
  };
  auto to_path = [&](PathStr const &path) -> PropertyPath {
    return PropertyPath{path | rv::transform(to_propertyid) | r::to_vector};
  };
  return property_paths_ | rv::transform(to_path) | r::to_vector;
}
}  // namespace memgraph::storage::durability
