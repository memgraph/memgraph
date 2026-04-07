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

struct TextIndexData {
  mutable mgcxx::text_search::Context context;
  LabelId scope;
  std::vector<PropertyId> properties;
  std::mutex write_mutex;  // Only used for exclusive locking during writes. IndexReader and IndexWriter are
                           // independent, so no lock is required when reading.

  TextIndexData(mgcxx::text_search::Context context, LabelId scope, std::vector<PropertyId> properties)
      : context(std::move(context)), scope(scope), properties(std::move(properties)) {}
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

  void CreateIndex(const TextIndexSpec &index_info, VerticesIterable vertices, NameIdMapper *name_id_mapper,
                   ActiveIndicesUpdater const &updater);

  void RecoverIndex(const TextIndexSpec &index_info, utils::SkipList<Vertex>::Accessor vertices,
                    NameIdMapper *name_id_mapper,
                    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  void DropIndex(const std::string &index_name, ActiveIndicesUpdater const &updater);

  bool IndexExists(const std::string &index_name) const;

  /// ListIndices on the owning class (for snapshot creation, outside transaction context).
  std::vector<TextIndexSpec> ListIndices() const;

  void Clear();

 private:
  std::filesystem::path text_index_storage_dir_;
  std::shared_ptr<IndexContainer> index_;

  void CreateTantivyIndex(const std::string &index_path, const TextIndexSpec &index_info);
  void PublishActiveIndices(ActiveIndicesUpdater const &updater);

  static void AddNodeToTextIndex(std::int64_t gid, nlohmann::json properties, std::string all_property_values,
                                 mgcxx::text_search::Context &context);
};

}  // namespace memgraph::storage
