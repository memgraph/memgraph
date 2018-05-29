#pragma once

#include <thread>

#include "distributed/data_rpc_clients.hpp"
#include "distributed/token_sharing_rpc_messages.hpp"
#include "storage/dynamic_graph_partitioner/vertex_migrator.hpp"
#include "storage/vertex_accessor.hpp"

namespace database {
class GraphDb;
class GraphDbAccessor;
};  // namespace database

/// Handles dynamic graph partitions, migrates vertices from one worker to
/// another based on available scoring which takes into account neighbours of a
/// vertex and tries to put it where most of its neighbours are located. Also
/// takes into account the number of vertices on the destination and source
/// machine.
class DynamicGraphPartitioner {
 public:
  DynamicGraphPartitioner(const DynamicGraphPartitioner &other) = delete;
  DynamicGraphPartitioner(DynamicGraphPartitioner &&other) = delete;
  DynamicGraphPartitioner &operator=(const DynamicGraphPartitioner &other) =
      delete;
  DynamicGraphPartitioner &operator=(DynamicGraphPartitioner &&other) = delete;

  explicit DynamicGraphPartitioner(database::GraphDb *db);

  /// Runs one dynamic graph partitioning cycle (step).
  void Run();

  /// Returns a vector of pairs of `vertex` and `destination` of where should
  /// some vertex be relocated from the view of `dba` accessor.
  //
  /// Each vertex is located on some worker (which in context of migrations we
  /// call a vertex label). Each vertex has it's score for each different label
  /// (worker_id) evaluated. This score is calculated by considering
  /// neighbouring vertices labels. Simply put, each vertex is attracted to be
  /// located on the same worker as it's neighbouring vertices. Migrations which
  /// improve that scoring, which also takes into account saturation of other
  /// workers on which it's considering to migrate this vertex, are determined.
  std::vector<std::pair<VertexAccessor, int>> FindMigrations(
      database::GraphDbAccessor &dba);

  /// Counts number of each label (worker_id) on endpoints of edges (in/out) of
  /// `vertex`.
  /// Returns a map consisting of (label, count) key-value pairs.
  std::unordered_map<int, int64_t> CountLabels(
      const VertexAccessor &vertex) const;

 private:
  database::GraphDb *db_;
};
