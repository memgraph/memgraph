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
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/snapshot_observer_info.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "text_search.hpp"

namespace memgraph::storage {

struct ActiveIndicesUpdater;
struct Transaction;

// TODO(follow-up): TextIndex and TextEdgeIndex are ~95% structurally identical
// (Data/IndexContainer/ActiveIndices/deferred_drop/Create/Drop/Recover/Clear).
// Extract a CRTP or template base before the duplication drifts.

struct TextIndexData {
  mutable mgcxx::text_search::Context context;
  LabelId scope;
  std::vector<PropertyId> properties;
  std::mutex write_mutex;  // Only used for exclusive locking during writes. IndexReader and IndexWriter are
                           // independent, so no lock is required when reading.

  /// When true, the destructor will call drop_index to clean up the tantivy index.
  /// Set by DropIndex so the context stays valid for existing snapshots until
  /// the last reference is released.
  bool deferred_drop{false};

  TextIndexData(mgcxx::text_search::Context context, LabelId scope, std::vector<PropertyId> properties)
      : context(std::move(context)), scope(scope), properties(std::move(properties)) {}

  ~TextIndexData();

  TextIndexData(const TextIndexData &) = delete;
  TextIndexData &operator=(const TextIndexData &) = delete;
  TextIndexData(TextIndexData &&) = delete;
  TextIndexData &operator=(TextIndexData &&) = delete;
};

struct TextSearchResult {
  Gid vertex_gid;
  double score;
};

/// Abstract interface for text index operations accessed through ActiveIndices snapshots.
struct TextIndexActiveIndices {
  virtual ~TextIndexActiveIndices() = default;

  virtual void UpdateOnAddLabel(LabelId label, const Vertex *vertex, Transaction &tx) = 0;
  virtual void UpdateOnRemoveLabel(LabelId label, const Vertex *vertex, Transaction &tx) = 0;
  virtual void UpdateOnSetProperty(const Vertex *vertex, Transaction &tx, PropertyId property) = 0;
  virtual void RemoveNode(const Vertex *vertex_after_update, Transaction &tx) = 0;

  virtual std::vector<TextSearchResult> Search(const std::string &index_name, const std::string &search_query,
                                               text_search_mode search_mode, std::size_t limit,
                                               const Transaction &tx) = 0;
  virtual std::string Aggregate(const std::string &index_name, const std::string &search_query,
                                const std::string &aggregation_query) = 0;

  virtual bool IndexExists(const std::string &index_name) const = 0;
  virtual std::vector<TextIndexSpec> ListIndices() const = 0;
  virtual std::optional<uint64_t> ApproximateVerticesTextCount(std::string_view index_name) const = 0;

  virtual void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) = 0;
};

using TextIndexActiveIndicesPtr = std::shared_ptr<TextIndexActiveIndices>;

class TextIndex {
 public:
  using IndexContainer = std::map<std::string, std::shared_ptr<TextIndexData>, std::less<>>;

  /// Concrete ActiveIndices implementation holding an immutable snapshot of the index container.
  struct ActiveIndices : TextIndexActiveIndices {
    explicit ActiveIndices(std::shared_ptr<IndexContainer const> container = std::make_shared<IndexContainer>())
        : index_container_(std::move(container)) {}

    void UpdateOnAddLabel(LabelId label, const Vertex *vertex, Transaction &tx) override;
    void UpdateOnRemoveLabel(LabelId label, const Vertex *vertex, Transaction &tx) override;
    void UpdateOnSetProperty(const Vertex *vertex, Transaction &tx, PropertyId property) override;
    void RemoveNode(const Vertex *vertex_after_update, Transaction &tx) override;

    std::vector<TextSearchResult> Search(const std::string &index_name, const std::string &search_query,
                                         text_search_mode search_mode, std::size_t limit,
                                         const Transaction &tx) override;
    std::string Aggregate(const std::string &index_name, const std::string &search_query,
                          const std::string &aggregation_query) override;

    bool IndexExists(const std::string &index_name) const override;
    std::vector<TextIndexSpec> ListIndices() const override;
    std::optional<uint64_t> ApproximateVerticesTextCount(std::string_view index_name) const override;

    void ApplyTrackedChanges(Transaction &tx, NameIdMapper *name_id_mapper) override;

   private:
    std::vector<TextIndexData *> LabelApplicableTextIndices(std::span<storage::LabelId const> labels) const;
    static std::vector<TextIndexData *> GetIndicesMatchingProperties(std::span<TextIndexData *const> label_indices,
                                                                     std::span<const PropertyId> properties);

    std::shared_ptr<IndexContainer const> index_container_;
  };

  explicit TextIndex(const std::filesystem::path &storage_dir)
      : text_index_storage_dir_(storage_dir / kTextIndicesDirectory), index_(std::make_shared<IndexContainer>()) {}

  TextIndex(const TextIndex &) = delete;
  TextIndex(TextIndex &&) = delete;
  TextIndex &operator=(const TextIndex &) = delete;
  TextIndex &operator=(TextIndex &&) = delete;

  ~TextIndex() = default;

  /// Returns the current active indices snapshot for use in transactions.
  auto GetActiveIndices() -> std::shared_ptr<TextIndexActiveIndices> { return std::make_shared<ActiveIndices>(index_); }

  /// Creates the index and populates it. Caller is responsible for publishing
  /// the resulting ActiveIndices snapshot via PublishActiveIndices — typically
  /// deferred through Transaction::commit_callbacks_ so the snapshot is only
  /// observable after the DDL transaction commits.
  void CreateIndex(const TextIndexSpec &index_info, VerticesIterable vertices, NameIdMapper *name_id_mapper);

  void RecoverIndex(const TextIndexSpec &index_info, utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper, ActiveIndicesUpdater const &updater,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// Removes the index from the live container and returns the evicted
  /// TextIndexData. The caller MUST keep the returned shared_ptr until commit
  /// time and only then flip its `deferred_drop` flag — doing so earlier
  /// (or in DropIndex itself) would destroy the on-disk tantivy directory
  /// if the DDL transaction later aborts, because existing ActiveIndices
  /// snapshots still alias the same TextIndexData.
  [[nodiscard]] std::shared_ptr<TextIndexData> DropIndex(const std::string &index_name);

  bool IndexExists(const std::string &index_name) const;

  /// ListIndices on the owning class (for snapshot creation, outside transaction context).
  std::vector<TextIndexSpec> ListIndices() const;

  /// Empties the live container and returns the evicted TextIndexData entries.
  /// The caller owns the flip-and-publish contract: match DropIndex and only
  /// flip `deferred_drop` on each returned entry when it's safe to unlink the
  /// on-disk tantivy directory (i.e. from a commit callback, or from
  /// IN_MEMORY_ANALYTICAL DropGraph where aborts can't happen). Does NOT
  /// publish to ActiveIndicesStore — the caller is responsible for that too.
  [[nodiscard]] std::vector<std::shared_ptr<TextIndexData>> Clear();

  /// Publishes the current index container as the new ActiveIndices snapshot.
  /// Safe to call any time; read operations see the latest published snapshot.
  void PublishActiveIndices(ActiveIndicesUpdater const &updater);

 private:
  std::filesystem::path text_index_storage_dir_;
  // Invariant: `index_` is only mutated under UNIQUE storage access (see the MG_ASSERTs in
  // Storage::Accessor::CreateTextIndex / DropTextIndex and in DropGraphClearIndices). Reads from
  // other contexts (regular READ/WRITE accessors, DatabaseInfoQuery) MUST go through the published
  // snapshot in `ActiveIndicesStore` -- UNIQUE excludes READ/WRITE, which is what makes direct
  // access to `index_` from commit-time hot paths (UpdateOnAddLabel etc.) race-free.
  std::shared_ptr<IndexContainer> index_;

  void CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info);

  static void AddNodeToTextIndex(std::int64_t gid, nlohmann::json properties, std::string all_property_values,
                                 mgcxx::text_search::Context &context);
};

}  // namespace memgraph::storage
