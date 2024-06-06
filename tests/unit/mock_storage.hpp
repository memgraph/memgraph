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

#include <gmock/gmock.h>

#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/vertices_iterable.hpp"

namespace memgraph::storage {

class MockStorage : public Storage {
 public:
  MockStorage() : Storage(Config(), StorageMode::IN_MEMORY_TRANSACTIONAL) {}

  MOCK_METHOD(void, FreeMemory, (std::unique_lock<utils::ResourceLock> main_guard, bool periodic), (override));

  MOCK_METHOD(std::unique_ptr<Accessor>, Access,
              (memgraph::replication_coordination_glue::ReplicationRole replication_role,
               std::optional<IsolationLevel> override_isolation_level),
              (override));

  MOCK_METHOD(std::unique_ptr<Accessor>, UniqueAccess,
              (memgraph::replication_coordination_glue::ReplicationRole replication_role,
               std::optional<IsolationLevel> override_isolation_level),
              (override));
  std::unique_ptr<Accessor> UniqueAccess(memgraph::replication_coordination_glue::ReplicationRole replication_role) {
    return UniqueAccess(replication_role, {});
  }

  MOCK_METHOD(StorageInfo, GetBaseInfo, (), (override));

  MOCK_METHOD(StorageInfo, GetInfo, (memgraph::replication_coordination_glue::ReplicationRole replication_role),
              (override));

  MOCK_METHOD(Transaction, CreateTransaction,
              (IsolationLevel isolation_level, StorageMode storage_mode,
               memgraph::replication_coordination_glue::ReplicationRole replication_role),
              (override));

  MOCK_METHOD(void, PrepareForNewEpoch, (), (override));
};

class MockStorageAccessor : public Storage::Accessor {
 public:
  MockStorageAccessor(Storage *storage)
      : Storage::Accessor(Accessor::shared_access, storage, IsolationLevel::SNAPSHOT_ISOLATION,
                          StorageMode::IN_MEMORY_TRANSACTIONAL,
                          memgraph::replication_coordination_glue::ReplicationRole::MAIN) {}

  MOCK_METHOD(VertexAccessor, CreateVertex, (), (override));

  MOCK_METHOD(std::optional<VertexAccessor>, FindVertex, (Gid gid, View view), (override));

  MOCK_METHOD(VerticesIterable, Vertices, (View view), (override));

  MOCK_METHOD(VerticesIterable, Vertices, (LabelId label, View view), (override));

  MOCK_METHOD(VerticesIterable, Vertices, (LabelId label, PropertyId property, View view), (override));

  MOCK_METHOD(VerticesIterable, Vertices, (LabelId label, PropertyId property, const PropertyValue &value, View view),
              (override));

  MOCK_METHOD(VerticesIterable, Vertices,
              (LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
               const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view),
              (override));

  MOCK_METHOD(std::optional<EdgeAccessor>, FindEdge, (Gid gid, View view), (override));

  MOCK_METHOD(EdgesIterable, Edges, (EdgeTypeId edge_type, View view), (override));

  MOCK_METHOD(uint64_t, ApproximateVertexCount, (), (override, const));

  MOCK_METHOD(uint64_t, ApproximateVertexCount, (LabelId label), (override, const));

  MOCK_METHOD(uint64_t, ApproximateVertexCount, (LabelId label, PropertyId property), (override, const));

  MOCK_METHOD(uint64_t, ApproximateVertexCount, (LabelId label, PropertyId property, const PropertyValue &value),
              (override, const));

  MOCK_METHOD(uint64_t, ApproximateVertexCount,
              (LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
               const std::optional<utils::Bound<PropertyValue>> &upper),
              (override, const));

  MOCK_METHOD(uint64_t, ApproximateEdgeCount, (EdgeTypeId id), (override, const));

  MOCK_METHOD(std::optional<LabelIndexStats>, GetIndexStats, (const LabelId &label), (override, const));

  MOCK_METHOD(std::optional<LabelPropertyIndexStats>, GetIndexStats, (const LabelId &label, const PropertyId &property),
              (override, const));

  MOCK_METHOD(void, SetIndexStats, (const LabelId &label, const LabelIndexStats &stats), (override));

  MOCK_METHOD(void, SetIndexStats,
              (const LabelId &label, const PropertyId &property, const LabelPropertyIndexStats &stats), (override));

  MOCK_METHOD((std::vector<std::pair<LabelId, PropertyId>>), DeleteLabelPropertyIndexStats, (const LabelId &label),
              (override));

  MOCK_METHOD(bool, DeleteLabelIndexStats, (const LabelId &label), (override));

  MOCK_METHOD(Result<EdgeAccessor>, CreateEdge, (VertexAccessor * from, VertexAccessor *to, EdgeTypeId edge_type),
              (override));

  MOCK_METHOD(std::optional<EdgeAccessor>, FindEdge,
              (Gid gid, View view, EdgeTypeId edge_type, VertexAccessor *from_vertex, VertexAccessor *to_vertex),
              (override));

  MOCK_METHOD(Result<EdgeAccessor>, EdgeSetFrom, (EdgeAccessor * edge, VertexAccessor *new_from), (override));

  MOCK_METHOD(Result<EdgeAccessor>, EdgeSetTo, (EdgeAccessor * edge, VertexAccessor *new_to), (override));

  MOCK_METHOD(Result<EdgeAccessor>, EdgeChangeType, (EdgeAccessor * edge, EdgeTypeId new_edge_type), (override));

  MOCK_METHOD(bool, LabelIndexExists, (LabelId label), (override, const));

  MOCK_METHOD(bool, LabelPropertyIndexExists, (LabelId label, PropertyId property), (override, const));

  MOCK_METHOD(bool, EdgeTypeIndexExists, (EdgeTypeId edge_type), (override, const));

  MOCK_METHOD(IndicesInfo, ListAllIndices, (), (override, const));

  MOCK_METHOD(ConstraintsInfo, ListAllConstraints, (), (override, const));

  // NOLINTNEXTLINE(google-default-arguments)
  MOCK_METHOD((utils::BasicResult<StorageManipulationError, void>), Commit,
              (CommitReplArgs reparg, DatabaseAccessProtector db_acc), (override));

  MOCK_METHOD(void, Abort, (), (override));

  MOCK_METHOD(void, FinalizeTransaction, (), (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), CreateIndex,
              (LabelId label, bool unique_access_needed), (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), CreateIndex,
              (LabelId label, PropertyId property), (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), CreateIndex,
              (EdgeTypeId edge_type, bool unique_access_needed), (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), DropIndex, (LabelId label), (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), DropIndex, (LabelId label, PropertyId property),
              (override));

  MOCK_METHOD((utils::BasicResult<StorageIndexDefinitionError, void>), DropIndex, (EdgeTypeId edge_type), (override));

  MOCK_METHOD((utils::BasicResult<StorageExistenceConstraintDefinitionError, void>), CreateExistenceConstraint,
              (LabelId label, PropertyId property), (override));

  MOCK_METHOD((utils::BasicResult<StorageExistenceConstraintDroppingError, void>), DropExistenceConstraint,
              (LabelId label, PropertyId property), (override));

  MOCK_METHOD((utils::BasicResult<StorageUniqueConstraintDefinitionError, UniqueConstraints::CreationStatus>),
              CreateUniqueConstraint, (LabelId label, const std::set<PropertyId> &properties), (override));

  MOCK_METHOD(UniqueConstraints::DeletionStatus, DropUniqueConstraint,
              (LabelId label, const std::set<PropertyId> &properties), (override));

  MOCK_METHOD(void, DropGraph, (), (override));
};

}  // namespace memgraph::storage
