/// @file

#pragma once

#include <thread>

#include "distributed/data_rpc_clients.hpp"
#include "distributed/token_sharing_rpc_messages.hpp"
#include "distributed/dgp/vertex_migrator.hpp"
#include "storage/vertex_accessor.hpp"

namespace database {
class GraphDb;
class GraphDbAccessor;
};  // namespace database

namespace distributed::dgp {

/// Contains a set of vertices and where they should be migrated
/// (machine/instance id) + score how good the partitioning is.
struct MigrationsData {
 private:
  using Migrations = std::vector<std::pair<VertexAccessor, int>>;

 public:
  MigrationsData(double score, Migrations migrations = Migrations())
      : score(std::move(score)), migrations(std::move(migrations)) {}

  /// Disable copying because the number of migrations could be huge. The
  /// expected number is 1k, but a user can configure the database in a way
  /// where the number of migrations could be much higher.
  MigrationsData(const MigrationsData &other) = delete;
  MigrationsData &operator=(const MigrationsData &other) = delete;

  MigrationsData(MigrationsData &&other) = default;
  MigrationsData &operator=(MigrationsData &&other) = default;

  double score;
  Migrations migrations;
};

/// Handles dynamic graph partitions, migrates vertices from one worker to
/// another based on available scoring which takes into account neighbours of a
/// vertex and tries to put it where most of its neighbours are located. Also
/// takes into account the number of vertices on the destination and source
/// machine.
class Partitioner {
 public:
  /// The partitioner needs GraphDb because each partition step is a new
  /// database transactions (database accessor has to be created).
  /// TODO (buda): Consider passing GraphDbAccessor directly.
  explicit Partitioner(database::GraphDb *db);

  Partitioner(const Partitioner &other) = delete;
  Partitioner(Partitioner &&other) = delete;
  Partitioner &operator=(const Partitioner &other) = delete;
  Partitioner &operator=(Partitioner &&other) = delete;

  /// Runs one dynamic graph partitioning cycle (step). In case of any error,
  /// the transaction will be aborted.
  ///
  /// @return Calculated partitioning score and were the migrations successful.
  std::pair<double, bool> Partition();

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
  MigrationsData FindMigrations(database::GraphDbAccessor &dba);

  /// Counts number of each label (worker_id) on endpoints of edges (in/out) of
  /// `vertex`.
  ///
  /// @return A map consisting of (label/machine/instance id, count) key-value
  ///         pairs.
  std::unordered_map<int, int64_t> CountLabels(
      const VertexAccessor &vertex) const;

 private:
  database::GraphDb *db_{nullptr};
};

}  // namespace distributed::dgp
