/// @file

#pragma once

#include <thread>
#include <unordered_map>

#include "storage/distributed/gid.hpp"
#include "storage/distributed/vertex_accessor.hpp"

namespace database {
class GraphDbAccessor;
};  // namespace database

namespace distributed::dgp {

/// Migrates vertices from one worker to another (updates edges as well).
class VertexMigrator {
 public:
  explicit VertexMigrator(database::GraphDbAccessor *dba);

  VertexMigrator(const VertexMigrator &other) = delete;
  VertexMigrator(VertexMigrator &&other) = delete;
  VertexMigrator &operator=(const VertexMigrator &other) = delete;
  VertexMigrator &operator=(VertexMigrator &&other) = delete;

  /// Creates a new vertex on the destination, deletes the old `vertex`, and
  /// deletes/creates every new edge that it needs since the destination of the
  /// vertex changed.
  void MigrateVertex(VertexAccessor &v, int destination);

 private:
  database::GraphDbAccessor *dba_;
  std::unordered_map<gid::Gid, storage::VertexAddress> vertex_migrated_to_;
};

}  // namespace distributed::dgp
