// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/durability/wal_schema_delta_apply.hpp"

#include <functional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <spdlog/spdlog.h>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>

#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/constraints/unique_constraints.hpp"
#include "storage/v2/description_store.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/indices/vector_index_utils.hpp"
#include "storage/v2/indices/vector_match_mode.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/ttl.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"

namespace memgraph::storage {

namespace {

namespace r = ranges;
namespace rv = r::views;

// Ported verbatim from InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn's schema-plane arms
// (see wal_schema_delta_apply.hpp file header). Each helper takes the already-resolved `accessor`
// (the caller has already picked the right StorageAccessType hint for this delta kind) plus whatever
// else the original inline lambda captured -- `storage` for NameToLabel/NameToProperty/NameToEdgeType
// and name_id_mapper_, `current_delta_idx` for trace logging.

void ApplyLabelIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                           durability::WalLabelIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create label index on :{}", current_delta_idx, data.label);
  if (!accessor->CreateIndex(storage->NameToLabel(data.label)))
    throw utils::BasicException("Failed to create label index on :{}.", data.label);
}

void ApplyLabelIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                         durability::WalLabelIndexDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop label index on :{}", current_delta_idx, data.label);
  if (!accessor->DropIndex(storage->NameToLabel(data.label)))
    throw utils::BasicException("Failed to drop label index on :{}.", data.label);
}

void ApplyLabelIndexStatsSet(ReplicationAccessor *accessor, InMemoryStorage *storage,
                             durability::WalLabelIndexStatsSet const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Set label index statistics on :{}", current_delta_idx, data.label);
  const auto label = storage->NameToLabel(data.label);
  LabelIndexStats stats{};
  if (!FromJson(data.json_stats, stats)) {
    throw utils::BasicException("Failed to read statistics!");
  }
  accessor->SetIndexStats(label, stats);
}

void ApplyLabelIndexStatsClear(ReplicationAccessor *accessor, InMemoryStorage *storage,
                               durability::WalLabelIndexStatsClear const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Clear label index statistics on :{}", current_delta_idx, data.label);
  if (!accessor->DeleteLabelIndexStats(storage->NameToLabel(data.label))) {
    throw utils::BasicException("Failed to clear label index statistics on :{}.", data.label);
  }
}

void ApplyLabelPropertyIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage, NameIdMapper *mapper,
                                   durability::WalLabelPropertyIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create label+property index on :{} ({})",
                current_delta_idx,
                data.label,
                data.composite_property_paths);
  auto property_paths = data.composite_property_paths.convert(mapper);
  if (!accessor->CreateIndex(storage->NameToLabel(data.label), std::move(property_paths)))
    throw utils::BasicException(
        "Failed to create label+property index on :{} ({}).", data.label, data.composite_property_paths);
}

void ApplyLabelPropertyIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage, NameIdMapper *mapper,
                                 durability::WalLabelPropertyIndexDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop label+property index on :{} ({})",
                current_delta_idx,
                data.label,
                data.composite_property_paths);
  auto property_paths = data.composite_property_paths.convert(mapper);
  if (!accessor->DropIndex(storage->NameToLabel(data.label), std::move(property_paths))) {
    throw utils::BasicException(
        "Failed to drop label+property index on :{} ({}).", data.label, data.composite_property_paths);
  }
}

void ApplyLabelPropertyIndexStatsSet(ReplicationAccessor *accessor, InMemoryStorage *storage, NameIdMapper *mapper,
                                     durability::WalLabelPropertyIndexStatsSet const &data,
                                     uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Set label-property index statistics on :{}", current_delta_idx, data.label);
  const auto label = storage->NameToLabel(data.label);
  auto property_paths = data.composite_property_paths.convert(mapper);
  LabelPropertyIndexStats stats{};
  if (!FromJson(data.json_stats, stats)) {
    throw utils::BasicException("Failed to read statistics!");
  }
  accessor->SetIndexStats(label, std::move(property_paths), stats);
}

void ApplyLabelPropertyIndexStatsClear(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                       durability::WalLabelPropertyIndexStatsClear const &data,
                                       uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Clear label-property index statistics on :{}", current_delta_idx, data.label);
  accessor->DeleteLabelPropertyIndexStats(storage->NameToLabel(data.label));
}

void ApplyEdgeTypeIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                              durability::WalEdgeTypeIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create edge index on :{}", current_delta_idx, data.edge_type);
  if (!accessor->CreateIndex(storage->NameToEdgeType(data.edge_type))) {
    throw utils::BasicException("Failed to create edge index on :{}.", data.edge_type);
  }
}

void ApplyEdgeTypeIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                            durability::WalEdgeTypeIndexDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop edge index on :{}", current_delta_idx, data.edge_type);
  if (!accessor->DropIndex(storage->NameToEdgeType(data.edge_type))) {
    throw utils::BasicException("Failed to drop edge index on :{}.", data.edge_type);
  }
}

void ApplyEdgeTypePropertyIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                      durability::WalEdgeTypePropertyIndexCreate const &data,
                                      uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
  if (!accessor->CreateIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
           .has_value()) {
    throw utils::BasicException("Failed to create edge property index on :{}({}).", data.edge_type, data.property);
  }
}

void ApplyEdgeTypePropertyIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                    durability::WalEdgeTypePropertyIndexDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
  if (!accessor->DropIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
           .has_value()) {
    throw utils::BasicException("Failed to drop edge property index on :{}({}).", data.edge_type, data.property);
  }
}

void ApplyEdgePropertyIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                  durability::WalEdgePropertyIndexCreate const &data, uint64_t /*current_delta_idx*/) {
  spdlog::trace("       Create global edge index on ({})", data.property);
  if (!accessor->CreateGlobalEdgeIndex(storage->NameToProperty(data.property))) {
    throw utils::BasicException("Failed to create global edge property index on ({}).", data.property);
  }
}

void ApplyEdgePropertyIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                durability::WalEdgePropertyIndexDrop const &data, uint64_t /*current_delta_idx*/) {
  spdlog::trace("       Drop global edge index on ({})", data.property);
  if (!accessor->DropGlobalEdgeIndex(storage->NameToProperty(data.property))) {
    throw utils::BasicException("Failed to drop global edge property index on ({}).", data.property);
  }
}

void ApplyTextIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                          durability::WalTextIndexCreate const &data, uint64_t current_delta_idx) {
  auto label_id = storage->NameToLabel(data.label);
  const auto properties_str = std::invoke([&]() -> std::string {
    if (data.properties && !data.properties->empty()) {
      return fmt::format(" ({})", rv::join(*data.properties, ", ") | r::to<std::string>);
    }
    return {};
  });
  spdlog::trace("   Delta {}. Create text search index {} on :{}{}",
                current_delta_idx,
                data.index_name,
                data.label,
                properties_str);
  auto prop_ids = std::invoke([&]() -> std::vector<PropertyId> {
    if (!data.properties) {
      return {};
    }
    return *data.properties | rv::transform([&](const auto &prop_name) { return storage->NameToProperty(prop_name); }) |
           r::to_vector;
  });
  auto ret = accessor->CreateTextIndex(storage::TextIndexSpec{data.index_name, label_id, prop_ids});
  if (!ret) {
    throw utils::BasicException("Failed to create text search index {} on {}.", data.index_name, data.label);
  }
}

void ApplyTextEdgeIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                              durability::WalTextEdgeIndexCreate const &data, uint64_t current_delta_idx) {
  const auto edge_type = storage->NameToEdgeType(data.edge_type);
  const auto properties_str = std::invoke([&]() -> std::string {
    if (!data.properties.empty()) {
      return fmt::format(" ({})", rv::join(data.properties, ", ") | r::to<std::string>);
    }
    return {};
  });
  spdlog::trace("   Delta {}. Create text search index {} on :{}{}",
                current_delta_idx,
                data.index_name,
                data.edge_type,
                properties_str);
  auto prop_ids = data.properties |
                  rv::transform([&](const auto &prop_name) { return storage->NameToProperty(prop_name); }) |
                  r::to_vector;
  const auto ret = accessor->CreateTextEdgeIndex(storage::TextEdgeIndexSpec{data.index_name, edge_type, prop_ids});
  if (!ret) {
    throw utils::BasicException("Failed to create text search index {} on {}.", data.index_name, data.edge_type);
  }
}

void ApplyTextIndexDrop(ReplicationAccessor *accessor, durability::WalTextIndexDrop const &data,
                        uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop text search index {}.", current_delta_idx, data.index_name);
  if (!accessor->DropTextIndex(data.index_name)) {
    throw utils::BasicException("Failed to drop text search index {}.", data.index_name);
  }
}

void ApplyExistenceConstraintCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                    durability::WalExistenceConstraintCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create existence constraint on :{} ({})", current_delta_idx, data.label, data.property);
  auto ret =
      accessor->CreateExistenceConstraint(storage->NameToLabel(data.label), storage->NameToProperty(data.property));
  if (!ret) {
    throw utils::BasicException("Failed to create existence constraint on :{} ({}).", data.label, data.property);
  }
}

void ApplyExistenceConstraintDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                  durability::WalExistenceConstraintDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop existence constraint on :{} ({})", current_delta_idx, data.label, data.property);
  if (!accessor->DropExistenceConstraint(storage->NameToLabel(data.label), storage->NameToProperty(data.property))
           .has_value()) {
    throw utils::BasicException("Failed to drop existence constraint on :{} ({}).", data.label, data.property);
  }
}

void ApplyUniqueConstraintCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                 durability::WalUniqueConstraintCreate const &data, uint64_t current_delta_idx) {
  std::stringstream ss;
  utils::PrintIterable(ss, data.properties);
  spdlog::trace("   Delta {}. Create unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
  std::set<PropertyId> properties;
  for (const auto &prop : data.properties) {
    properties.emplace(storage->NameToProperty(prop));
  }
  auto ret = accessor->CreateUniqueConstraint(storage->NameToLabel(data.label), properties);
  if (!ret || ret.value() != UniqueConstraints::CreationStatus::SUCCESS) {
    throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
  }
}

void ApplyUniqueConstraintDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                               durability::WalUniqueConstraintDrop const &data, uint64_t current_delta_idx) {
  std::stringstream ss;
  utils::PrintIterable(ss, data.properties);
  spdlog::trace("   Delta {}. Drop unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
  std::set<PropertyId> properties;
  for (const auto &prop : data.properties) {
    properties.emplace(storage->NameToProperty(prop));
  }
  auto ret = accessor->DropUniqueConstraint(storage->NameToLabel(data.label), properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
  }
}

void ApplyTypeConstraintCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                               durability::WalTypeConstraintCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create IS TYPED {} constraint on :{} ({})",
                current_delta_idx,
                storage::TypeConstraintKindToString(data.kind),
                data.label,
                data.property);

  auto ret = accessor->CreateTypeConstraint(
      storage->NameToLabel(data.label), storage->NameToProperty(data.property), data.kind);
  if (!ret) {
    throw utils::BasicException("Failed to create IS TYPED {} constraint on :{} ({}).",
                                TypeConstraintKindToString(data.kind),
                                data.label,
                                data.property);
  }
}

void ApplyTypeConstraintDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                             durability::WalTypeConstraintDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop IS TYPED {} constraint on :{} ({})",
                current_delta_idx,
                TypeConstraintKindToString(data.kind),
                data.label,
                data.property);

  auto ret =
      accessor->DropTypeConstraint(storage->NameToLabel(data.label), storage->NameToProperty(data.property), data.kind);
  if (!ret) {
    throw utils::BasicException("Failed to drop IS TYPED {} constraint on :{} ({}).",
                                TypeConstraintKindToString(data.kind),
                                data.label,
                                data.property);
  }
}

void ApplyEnumCreate(ReplicationAccessor *accessor, durability::WalEnumCreate const &data, uint64_t current_delta_idx) {
  std::stringstream ss;
  utils::PrintIterable(ss, data.evalues);
  spdlog::trace("   Delta {}. Create enum {} with values {}", current_delta_idx, data.etype, ss.str());
  auto res = accessor->CreateEnum(data.etype, data.evalues);
  if (!res) {
    throw utils::BasicException("Failed to create enum {} with values {}.", data.etype, ss.str());
  }
}

void ApplyEnumAlterAdd(ReplicationAccessor *accessor, durability::WalEnumAlterAdd const &data,
                       uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Alter enum {} add value {}", current_delta_idx, data.etype, data.evalue);
  auto res = accessor->EnumAlterAdd(data.etype, data.evalue);
  if (!res) {
    throw utils::BasicException("Failed to alter enum {} add value {}.", data.etype, data.evalue);
  }
}

void ApplyEnumAlterUpdate(ReplicationAccessor *accessor, durability::WalEnumAlterUpdate const &data,
                          uint64_t current_delta_idx) {
  spdlog::trace(
      "   Delta {}. Alter enum {} update {} to {}", current_delta_idx, data.etype, data.evalue_old, data.evalue_new);
  auto res = accessor->EnumAlterUpdate(data.etype, data.evalue_old, data.evalue_new);
  if (!res) {
    throw utils::BasicException(
        "Failed to alter enum {} update {} to {}.", data.etype, data.evalue_old, data.evalue_new);
  }
}

void ApplyPointIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                           durability::WalPointIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Create point index on :{}({})", current_delta_idx, data.label, data.property);
  auto labelId = storage->NameToLabel(data.label);
  auto propId = storage->NameToProperty(data.property);
  auto res = accessor->CreatePointIndex(labelId, propId);
  if (!res) {
    throw utils::BasicException("Failed to create point index on :{}({})", data.label, data.property);
  }
}

void ApplyPointIndexDrop(ReplicationAccessor *accessor, InMemoryStorage *storage,
                         durability::WalPointIndexDrop const &data, uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop point index on :{}({})", current_delta_idx, data.label, data.property);
  auto labelId = storage->NameToLabel(data.label);
  auto propId = storage->NameToProperty(data.property);
  auto res = accessor->DropPointIndex(labelId, propId);
  if (!res) {
    throw utils::BasicException("Failed to drop point index on :{}({})", data.label, data.property);
  }
}

void ApplyVectorIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                            durability::WalVectorIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace(
      "   Delta {}. Create vector index {} on property {}", current_delta_idx, data.index_name, data.property);
  auto propId = storage->NameToProperty(data.property);
  auto metric_kind = storage::MetricFromName(data.metric_kind);
  auto scalar_kind = data.scalar_kind ? static_cast<unum::usearch::scalar_kind_t>(*data.scalar_kind)
                                      : unum::usearch::scalar_kind_t::f32_k;

  std::vector<storage::LabelId> label_ids;
  label_ids.reserve(data.label_filter.ids.size());
  for (const auto &name : data.label_filter.ids) label_ids.push_back(storage->NameToLabel(name));

  auto res = accessor->CreateVectorIndex(storage::VectorIndexSpec{
      .index_name = data.index_name,
      .label_filter = storage::VectorLabelFilter{.mode = static_cast<storage::VectorMatchMode>(data.label_filter.mode),
                                                 .ids = std::move(label_ids)},
      .property = propId,
      .metric_kind = metric_kind,
      .dimension = data.dimension,
      .resize_coefficient = data.resize_coefficient,
      .capacity = data.capacity,
      .scalar_kind = scalar_kind,
  });
  if (!res) {
    throw utils::BasicException("Failed to create vector index {} on property {}", data.index_name, data.property);
  }
}

void ApplyVectorEdgeIndexCreate(ReplicationAccessor *accessor, InMemoryStorage *storage,
                                durability::WalVectorEdgeIndexCreate const &data, uint64_t current_delta_idx) {
  spdlog::trace(
      "   Delta {}. Create vector edge index {} on property {}", current_delta_idx, data.index_name, data.property);
  auto propId = storage->NameToProperty(data.property);
  auto metric_kind = storage::MetricFromName(data.metric_kind);

  std::vector<storage::EdgeTypeId> edge_type_ids;
  edge_type_ids.reserve(data.edge_type_filter.ids.size());
  for (const auto &name : data.edge_type_filter.ids) edge_type_ids.push_back(storage->NameToEdgeType(name));

  auto res = accessor->CreateVectorEdgeIndex(storage::VectorEdgeIndexSpec{
      .index_name = data.index_name,
      .edge_type_filter =
          storage::VectorEdgeTypeFilter{.mode = static_cast<storage::VectorMatchMode>(data.edge_type_filter.mode),
                                        .ids = std::move(edge_type_ids)},
      .property = propId,
      .metric_kind = metric_kind,
      .dimension = data.dimension,
      .resize_coefficient = data.resize_coefficient,
      .capacity = data.capacity,
      .scalar_kind = static_cast<unum::usearch::scalar_kind_t>(data.scalar_kind),
  });
  if (!res) {
    throw utils::BasicException("Failed to create vector edge index {} on property {}", data.index_name, data.property);
  }
}

void ApplyVectorIndexDrop(ReplicationAccessor *accessor, durability::WalVectorIndexDrop const &data,
                          uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Drop vector index {} ", current_delta_idx, data.index_name);
  auto res = accessor->DropVectorIndex(data.index_name);
  if (!res) {
    throw utils::BasicException("Failed to drop vector index {}", data.index_name);
  }
}

void ApplyTtlOperation(ReplicationAccessor *accessor, [[maybe_unused]] durability::WalTtlOperation const &data,
                       [[maybe_unused]] uint64_t current_delta_idx) {
#ifdef MG_ENTERPRISE
  spdlog::trace("   Delta {}. TTL operation type {}", current_delta_idx, static_cast<int>(data.operation_type));
  switch (data.operation_type) {
    case storage::durability::TtlOperationType::ENABLE:
      accessor->StartTtl({.is_main = false});
      break;
    case storage::durability::TtlOperationType::DISABLE:
      accessor->DisableTtl({.is_main = false});
      break;
    case storage::durability::TtlOperationType::CONFIGURE:
      accessor->ConfigureTtl(storage::ttl::TtlInfo{data.period, data.start_time, data.should_run_edge_ttl},
                             {.is_main = false});
      // Configuration will leave it paused; replicas should not run ttl
      break;
    case storage::durability::TtlOperationType::STOP:
      accessor->StopTtl();
      break;
    default:
      throw utils::BasicException("Invalid TTL operation type: {}", static_cast<int>(data.operation_type));
  }
#else
  spdlog::trace("TTL operation is not supported in community edition");
#endif
}

void ApplyDescriptionSet(ReplicationAccessor *accessor, durability::WalDescriptionSet const &data,
                         uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Set description (kind={})", current_delta_idx, static_cast<int>(data.kind));
  switch (data.kind) {
    case DescriptionTargetKind::DATABASE:
      accessor->SetDatabaseDescription(data.description);
      break;
    case DescriptionTargetKind::LABEL:
      accessor->SetLabelDescription(data.labels, data.description);
      break;
    case DescriptionTargetKind::EDGE_TYPE:
      accessor->SetEdgeTypeDescription(data.edge_type, data.description);
      break;
    case DescriptionTargetKind::LABEL_PROPERTY:
      accessor->SetLabelPropertyDescription(data.labels, data.property, data.description);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PROPERTY:
      accessor->SetEdgeTypePropertyDescription(data.edge_type, data.property, data.description);
      break;
    case DescriptionTargetKind::PROPERTY:
      accessor->SetPropertyDescription(data.property, data.description);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PATTERN:
      accessor->SetEdgeTypePatternDescription(data.from_labels, data.edge_type, data.to_labels, data.description);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY:
      accessor->SetEdgeTypePatternPropertyDescription(
          data.from_labels, data.edge_type, data.to_labels, data.property, data.description);
      break;
    default:
      break;
  }
}

void ApplyDescriptionDelete(ReplicationAccessor *accessor, durability::WalDescriptionDelete const &data,
                            uint64_t current_delta_idx) {
  spdlog::trace("   Delta {}. Delete description (kind={})", current_delta_idx, static_cast<int>(data.kind));
  switch (data.kind) {
    case DescriptionTargetKind::DATABASE:
      accessor->DeleteDatabaseDescription();
      break;
    case DescriptionTargetKind::LABEL:
      accessor->DeleteLabelDescription(data.labels);
      break;
    case DescriptionTargetKind::EDGE_TYPE:
      accessor->DeleteEdgeTypeDescription(data.edge_type);
      break;
    case DescriptionTargetKind::LABEL_PROPERTY:
      accessor->DeleteLabelPropertyDescription(data.labels, data.property);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PROPERTY:
      accessor->DeleteEdgeTypePropertyDescription(data.edge_type, data.property);
      break;
    case DescriptionTargetKind::PROPERTY:
      accessor->DeletePropertyDescription(data.property);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PATTERN:
      accessor->DeleteEdgeTypePatternDescription(data.from_labels, data.edge_type, data.to_labels);
      break;
    case DescriptionTargetKind::EDGE_TYPE_PATTERN_PROPERTY:
      accessor->DeleteEdgeTypePatternPropertyDescription(
          data.from_labels, data.edge_type, data.to_labels, data.property);
      break;
    default:
      break;
  }
}

}  // namespace

void ApplyWalSchemaDelta(ReplicationAccessor *accessor, InMemoryStorage *storage, durability::WalDeltaData const &delta,
                         uint64_t current_delta_idx) {
  using namespace durability;  // NOLINT (google-build-using-namespace) -- matches
                               // ApplyWalDataDelta's dispatch (and the original
                               // ReadAndApplyDeltasSingleTxn dispatch both were extracted from).
  auto *mapper = storage->name_id_mapper_.get();

  if (auto const *data = std::get_if<WalLabelIndexCreate>(&delta.data_)) {
    ApplyLabelIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelIndexDrop>(&delta.data_)) {
    ApplyLabelIndexDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelIndexStatsSet>(&delta.data_)) {
    ApplyLabelIndexStatsSet(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelIndexStatsClear>(&delta.data_)) {
    ApplyLabelIndexStatsClear(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelPropertyIndexCreate>(&delta.data_)) {
    ApplyLabelPropertyIndexCreate(accessor, storage, mapper, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelPropertyIndexDrop>(&delta.data_)) {
    ApplyLabelPropertyIndexDrop(accessor, storage, mapper, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelPropertyIndexStatsSet>(&delta.data_)) {
    ApplyLabelPropertyIndexStatsSet(accessor, storage, mapper, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalLabelPropertyIndexStatsClear>(&delta.data_)) {
    ApplyLabelPropertyIndexStatsClear(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeTypeIndexCreate>(&delta.data_)) {
    ApplyEdgeTypeIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeTypeIndexDrop>(&delta.data_)) {
    ApplyEdgeTypeIndexDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeTypePropertyIndexCreate>(&delta.data_)) {
    ApplyEdgeTypePropertyIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgeTypePropertyIndexDrop>(&delta.data_)) {
    ApplyEdgeTypePropertyIndexDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgePropertyIndexCreate>(&delta.data_)) {
    ApplyEdgePropertyIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEdgePropertyIndexDrop>(&delta.data_)) {
    ApplyEdgePropertyIndexDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTextIndexCreate>(&delta.data_)) {
    ApplyTextIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTextEdgeIndexCreate>(&delta.data_)) {
    ApplyTextEdgeIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTextIndexDrop>(&delta.data_)) {
    ApplyTextIndexDrop(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalExistenceConstraintCreate>(&delta.data_)) {
    ApplyExistenceConstraintCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalExistenceConstraintDrop>(&delta.data_)) {
    ApplyExistenceConstraintDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalUniqueConstraintCreate>(&delta.data_)) {
    ApplyUniqueConstraintCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalUniqueConstraintDrop>(&delta.data_)) {
    ApplyUniqueConstraintDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTypeConstraintCreate>(&delta.data_)) {
    ApplyTypeConstraintCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTypeConstraintDrop>(&delta.data_)) {
    ApplyTypeConstraintDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEnumCreate>(&delta.data_)) {
    ApplyEnumCreate(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEnumAlterAdd>(&delta.data_)) {
    ApplyEnumAlterAdd(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalEnumAlterUpdate>(&delta.data_)) {
    ApplyEnumAlterUpdate(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalPointIndexCreate>(&delta.data_)) {
    ApplyPointIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalPointIndexDrop>(&delta.data_)) {
    ApplyPointIndexDrop(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVectorIndexCreate>(&delta.data_)) {
    ApplyVectorIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVectorEdgeIndexCreate>(&delta.data_)) {
    ApplyVectorEdgeIndexCreate(accessor, storage, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalVectorIndexDrop>(&delta.data_)) {
    ApplyVectorIndexDrop(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalTtlOperation>(&delta.data_)) {
    ApplyTtlOperation(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalDescriptionSet>(&delta.data_)) {
    ApplyDescriptionSet(accessor, *data, current_delta_idx);
    return;
  }
  if (auto const *data = std::get_if<WalDescriptionDelete>(&delta.data_)) {
    ApplyDescriptionDelete(accessor, *data, current_delta_idx);
    return;
  }

  // Unreachable in practice: callers (replica-apply today) only invoke this function for the
  // schema-plane delta kinds handled above. Data-plane deltas go through storage::ApplyWalDataDelta
  // (wal_delta_apply.{hpp,cpp}, S3b); framing (WalTransactionStart/End) is dispatched by the caller
  // itself -- see the file header comment in wal_schema_delta_apply.hpp for why.
  throw utils::BasicException("ApplyWalSchemaDelta called with a non-schema-plane WAL delta.");
}

}  // namespace memgraph::storage
