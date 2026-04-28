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

#pragma once

#include <mutex>
#include <nlohmann/json_fwd.hpp>

#include "mg_procedure.h"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

struct Transaction;

struct ActiveIndicesUpdater;

// TODO(follow-up): TextIndex and TextEdgeIndex are ~95% structurally identical
// (Data/IndexContainer/ActiveIndices/deferred_drop/Create/Drop/Recover/Clear).
// Extract a CRTP or template base before the duplication drifts.

struct TextEdgeIndexData {
  mutable mgcxx::text_search::Context context;
  EdgeTypeId scope;
  std::vector<PropertyId> properties;
  std::mutex write_mutex;  // Only used for exclusive locking during writes. IndexReader and IndexWriter are
                           // independent, so no lock is required when reading.

  /// When true, the destructor will call drop_index to clean up the tantivy index.
  /// Set by DropIndex so the context stays valid for existing snapshots until
  /// the last reference is released.
  bool deferred_drop{false};

  TextEdgeIndexData(mgcxx::text_search::Context context, EdgeTypeId scope, std::vector<PropertyId> properties)
      : context(std::move(context)), scope(scope), properties(std::move(properties)) {}

  ~TextEdgeIndexData();

  TextEdgeIndexData(const TextEdgeIndexData &) = delete;
  TextEdgeIndexData &operator=(const TextEdgeIndexData &) = delete;
  TextEdgeIndexData(TextEdgeIndexData &&) = delete;
  TextEdgeIndexData &operator=(TextEdgeIndexData &&) = delete;
};

struct TextEdgeSearchResult {
  Gid edge_gid;
  Gid from_vertex_gid;
  Gid to_vertex_gid;
  double score;
};

/// Abstract interface for text edge index operations accessed through ActiveIndices snapshots.
struct TextEdgeIndexActiveIndices {
  virtual ~TextEdgeIndexActiveIndices() = default;

  virtual void RemoveEdge(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex, EdgeTypeId edge_type,
                          Transaction &tx) = 0;
  virtual void UpdateOnSetProperty(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex,
                                   EdgeTypeId edge_type, Transaction &tx, PropertyId property) = 0;

  virtual std::vector<TextEdgeSearchResult> Search(const std::string &index_name, const std::string &search_query,
                                                   text_search_mode search_mode, std::size_t limit,
                                                   const Transaction &tx) = 0;
  virtual std::string Aggregate(const std::string &index_name, const std::string &search_query,
                                const std::string &aggregation_query) = 0;

  virtual bool IndexExists(const std::string &index_name) const = 0;
  virtual std::vector<TextEdgeIndexSpec> ListIndices() const = 0;
  virtual std::optional<uint64_t> ApproximateEdgesTextCount(std::string_view index_name) const = 0;

  virtual void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) = 0;
};

using TextEdgeIndexActiveIndicesPtr = std::shared_ptr<TextEdgeIndexActiveIndices>;

class TextEdgeIndex {
 public:
  using IndexContainer = std::map<std::string, std::shared_ptr<TextEdgeIndexData>, std::less<>>;

  /// Concrete ActiveIndices implementation holding an immutable snapshot of the index container.
  struct ActiveIndices : TextEdgeIndexActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndexContainer const> container = std::make_shared<IndexContainer>())
        : index_container_(std::move(container)) {}

    void RemoveEdge(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex, EdgeTypeId edge_type,
                    Transaction &tx) override;
    void UpdateOnSetProperty(const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex, EdgeTypeId edge_type,
                             Transaction &tx, PropertyId property) override;

    std::vector<TextEdgeSearchResult> Search(const std::string &index_name, const std::string &search_query,
                                             text_search_mode search_mode, std::size_t limit,
                                             const Transaction &tx) override;
    std::string Aggregate(const std::string &index_name, const std::string &search_query,
                          const std::string &aggregation_query) override;

    bool IndexExists(const std::string &index_name) const override;
    std::vector<TextEdgeIndexSpec> ListIndices() const override;
    std::optional<uint64_t> ApproximateEdgesTextCount(std::string_view index_name) const override;

    void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) override;

   private:
    std::vector<TextEdgeIndexData *> EdgeTypeApplicableTextIndices(EdgeTypeId edge_type) const;
    static std::vector<TextEdgeIndexData *> GetIndicesMatchingProperties(
        std::span<TextEdgeIndexData *const> edge_type_indices, std::span<const PropertyId> properties);

    std::shared_ptr<IndexContainer const> index_container_;
  };

  explicit TextEdgeIndex(const std::filesystem::path &storage_dir)
      : text_index_storage_dir_(storage_dir / kTextIndicesDirectory), index_(std::make_shared<IndexContainer>()) {}

  TextEdgeIndex(const TextEdgeIndex &) = delete;
  TextEdgeIndex(TextEdgeIndex &&) = delete;
  TextEdgeIndex &operator=(const TextEdgeIndex &) = delete;
  TextEdgeIndex &operator=(TextEdgeIndex &&) = delete;

  ~TextEdgeIndex() = default;

  /// Returns the current active indices snapshot for use in transactions.
  auto GetActiveIndices() -> std::shared_ptr<TextEdgeIndexActiveIndices> {
    return std::make_shared<ActiveIndices>(index_);
  }

  /// Creates the index and populates it. Caller is responsible for publishing
  /// the resulting ActiveIndices snapshot via PublishActiveIndices — typically
  /// deferred through Transaction::commit_callbacks_ so the snapshot is only
  /// observable after the DDL transaction commits.
  void CreateIndex(const TextEdgeIndexSpec &index_info, VerticesIterable vertices, NameIdMapper *name_id_mapper);

  void RecoverIndex(const TextEdgeIndexSpec &index_info, utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper, ActiveIndicesUpdater const &updater,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// Removes the index from the live container and returns the evicted
  /// TextEdgeIndexData. The caller MUST keep the returned shared_ptr until
  /// commit time and only then flip its `deferred_drop` flag — doing so
  /// earlier (or in DropIndex itself) would destroy the on-disk tantivy
  /// directory if the DDL transaction later aborts, because existing
  /// ActiveIndices snapshots still alias the same TextEdgeIndexData.
  [[nodiscard]] std::shared_ptr<TextEdgeIndexData> DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  /// ListIndices on the owning class (for snapshot creation, outside transaction context).
  std::vector<TextEdgeIndexSpec> ListIndices() const;

  /// Empties the live container and returns the evicted TextEdgeIndexData
  /// entries. The caller owns the flip-and-publish contract: match DropIndex
  /// and only flip `deferred_drop` on each returned entry when it's safe to
  /// unlink the on-disk tantivy directory (i.e. from a commit callback, or
  /// from IN_MEMORY_ANALYTICAL DropGraph where aborts can't happen). Does NOT
  /// publish to ActiveIndicesStore — the caller is responsible for that too.
  [[nodiscard]] std::vector<std::shared_ptr<TextEdgeIndexData>> Clear();

  /// Publishes the current index container as the new ActiveIndices snapshot.
  /// Safe to call any time; read operations see the latest published snapshot.
  void PublishActiveIndices(ActiveIndicesUpdater const &updater);

 private:
  std::filesystem::path text_index_storage_dir_;
  // Invariant: `index_` is only mutated under UNIQUE storage access (see the MG_ASSERTs in
  // Storage::Accessor::CreateTextEdgeIndex / DropTextIndex and in DropGraphClearIndices). Reads
  // from other contexts (regular READ/WRITE accessors, DatabaseInfoQuery) MUST go through the
  // published snapshot in `ActiveIndicesStore` -- UNIQUE excludes READ/WRITE, which is what makes
  // direct access to `index_` from commit-time hot paths race-free.
  std::shared_ptr<IndexContainer> index_;

  void CreateTantivyIndex(const std::string &index_path, const TextEdgeIndexSpec &index_info);

  static void AddEdgeToTextIndex(std::int64_t edge_gid, std::int64_t from_vertex_gid, std::int64_t to_vertex_gid,
                                 nlohmann::json properties, std::string all_property_values,
                                 mgcxx::text_search::Context &context);
};

}  // namespace memgraph::storage
