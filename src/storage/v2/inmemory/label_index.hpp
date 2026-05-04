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

#include <span>

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/durability/recovery_type.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/errors.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/indices_mvcc.hpp"
#include "storage/v2/vertex.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

class InMemoryLabelIndex : public LabelIndex {
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) { return std::tie(vertex, timestamp) < std::tie(rhs.vertex, rhs.timestamp); }

    bool operator==(const Entry &rhs) const {
      return std::tie(vertex, timestamp) == std::tie(rhs.vertex, rhs.timestamp);
    }
  };

 public:
  struct IndividualIndex {
    explicit IndividualIndex() : skiplist{} {}

    ~IndividualIndex();
    void Publish(uint64_t commit_timestamp);
    utils::SkipListDb<Entry> skiplist;
    IndexStatus status{};
  };

  struct AllIndicesEntry {
    std::shared_ptr<IndividualIndex> index_;
    LabelId label_;
  };

  using IndexContainer = std::map<LabelId, std::shared_ptr<IndividualIndex>, std::less<LabelId>,
                                  memory::DbAwareAllocator<std::pair<const LabelId, std::shared_ptr<IndividualIndex>>>>;

  /// @throw std::bad_alloc
  bool CreateIndexOnePass(LabelId label, utils::SkipListDb<Vertex>::Accessor vertices,
                          const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                          ActiveIndicesUpdater const &updater,
                          std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt);

  /// Removes the index and returns the evicted IndividualIndex (nullptr if absent).
  /// Caller can re-install via RestoreIndex on abort. The returned shared_ptr keeps
  /// the entry alive in all_indices_, so RestoreIndex must not re-append there.
  [[nodiscard]] auto DropIndex(LabelId label, ActiveIndicesUpdater const &updater) -> std::shared_ptr<IndividualIndex>;
  void RestoreIndex(LabelId label, std::shared_ptr<IndividualIndex> evicted, ActiveIndicesUpdater const &updater);

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token);

  class Iterable {
   public:
    Iterable(utils::SkipListDb<Entry>::Accessor index_accessor,
             utils::SkipListDb<Vertex>::ConstAccessor vertices_accessor, LabelId label,
             View view, Storage *storage, Transaction *transaction);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipListDb<Entry>::Iterator index_iterator);

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipListDb<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return {this, index_accessor_.begin()}; }

    Iterator end() { return {this, index_accessor_.end()}; }

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipListDb<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
  };

  class ChunkedIterable {
   public:
    ChunkedIterable(utils::SkipListDb<Entry>::Accessor index_accessor,
                    utils::SkipListDb<Vertex>::ConstAccessor vertices_accessor,
                    LabelId label, View view, Storage *storage, Transaction *transaction, size_t num_chunks);

    class Iterator {
     public:
      Iterator(ChunkedIterable *self,
               utils::SkipListDb<Entry>::ChunkedIterator index_iterator)
          : self_(self), index_iterator_(index_iterator), current_vertex_accessor_(nullptr, self_->storage_, nullptr) {
        AdvanceUntilValid();
      }

      VertexAccessor const &operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }

      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++() {
        ++index_iterator_;
        AdvanceUntilValid();
        return *this;
      }

     private:
      void AdvanceUntilValid();

      ChunkedIterable *self_;
      utils::SkipListDb<Entry>::ChunkedIterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_{nullptr};
    };

    class Chunk {
      Iterator begin_;
      Iterator end_;

     public:
      Chunk(ChunkedIterable *self, utils::SkipListDb<Entry>::Chunk &chunk)
          : begin_{self, chunk.begin()}, end_{self, chunk.end()} {}

      Iterator begin() { return begin_; }

      Iterator end() { return end_; }
    };

    Chunk get_chunk(size_t id) { return {this, chunks_[id]}; }

    size_t size() const { return chunks_.size(); }

   private:
    utils::SkipListDb<Vertex>::ConstAccessor pin_accessor_;
    utils::SkipListDb<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Storage *storage_;
    Transaction *transaction_;
    utils::SkipListDb<Entry>::ChunkCollection chunks_;
  };

  struct ActiveIndices : LabelIndex::ActiveIndices {
    ActiveIndices(std::shared_ptr<const IndexContainer> index_container = std::make_shared<IndexContainer>())
        : index_container_{std::move(index_container)} {}

    /// @throw std::bad_alloc
    void UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update, const Transaction &tx) override;

    // Not used for in-memory
    void UpdateOnRemoveLabel(LabelId removed_label, Vertex *vertex_after_update, const Transaction &tx) override {};

    bool IndexRegistered(LabelId label) const override;

    bool IndexReady(LabelId label) const override;

    std::vector<LabelId> ListIndices(uint64_t start_timestamp) const override;

    uint64_t ApproximateVertexCount(LabelId label) const override;

    /// Surgical removal of entries that was inserted this transaction
    void AbortEntries(AbortableInfo const &, uint64_t start_timestamp) override;

    Iterable Vertices(LabelId label, View view, Storage *storage, Transaction *transaction);

    Iterable Vertices(LabelId label,
                      utils::SkipListDb<Vertex>::ConstAccessor vertices_acc, View view,
                      Storage *storage, Transaction *transaction);

    ChunkedIterable ChunkedVertices(LabelId label,
                                    utils::SkipListDb<Vertex>::ConstAccessor vertices_acc,
                                    View view, Storage *storage, Transaction *transaction, size_t num_chunks);

    auto GetAbortProcessor() const -> AbortProcessor override;

   private:
    std::shared_ptr<IndexContainer const> index_container_;
  };

  InMemoryLabelIndex() = default;

  auto GetActiveIndices() const -> std::shared_ptr<LabelIndex::ActiveIndices> override;

  auto RegisterIndex(LabelId, ActiveIndicesUpdater const &updater) -> bool;
  auto PopulateIndex(LabelId label, utils::SkipListDb<Vertex>::Accessor vertices,
                     const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
                     ActiveIndicesUpdater const &updater,
                     std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt,
                     Transaction const *tx = nullptr, CheckCancelFunction cancel_check = neverCancel)
      -> std::expected<void, IndexPopulateError>;
  bool PublishIndex(LabelId label, uint64_t commit_timestamp);

  void RunGC();

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats);

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const;

  std::vector<LabelId> ClearIndexStats();

  bool DeleteIndexStats(const storage::LabelId &label);

  void DropGraphClearIndices() override;

 private:
  auto CleanupAllIndices() -> void;
  auto GetIndividualIndex(LabelId label) const -> std::shared_ptr<IndividualIndex>;

  // Atomic install into index_ + (optional) all_indices_. Returns false if the slot
  // is taken. Shared by RegisterIndex (true) and RestoreIndex (false).
  bool InstallIndividualIndex_(LabelId label, std::shared_ptr<IndividualIndex> entry,
                               ActiveIndicesUpdater const &updater, bool register_in_all_indices);

  utils::Synchronized<std::shared_ptr<IndexContainer const>, utils::WritePrioritizedRWLock> index_{
      std::make_shared<IndexContainer const>()};
  utils::Synchronized<std::shared_ptr<std::vector<AllIndicesEntry> const>, utils::WritePrioritizedRWLock> all_indices_{
      std::make_shared<std::vector<AllIndicesEntry> const>()};
  utils::Synchronized<std::map<LabelId, storage::LabelIndexStats>, utils::ReadPrioritizedRWLock> stats_;
};

}  // namespace memgraph::storage
