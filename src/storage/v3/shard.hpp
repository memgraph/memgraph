// Copyright 2023 Memgraph Ltd.
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

#include <cstdint>
#include <filesystem>
#include <map>
#include <numeric>
#include <optional>
#include <shared_mutex>
#include <variant>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "io/network/endpoint.hpp"
#include "io/time.hpp"
#include "kvstore/kvstore.hpp"
#include "storage/v3/config.hpp"
#include "storage/v3/edge.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/name_id_mapper.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/result.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/schemas.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertex_id.hpp"
#include "storage/v3/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/file_locker.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/rw_lock.hpp"
#include "utils/scheduler.hpp"
#include "utils/skip_list.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::v3 {

// The storage is based on this paper:
// https://db.in.tum.de/~muehlbau/papers/mvcc.pdf
// The paper implements a fully serializable storage, in our implementation we
// only implement snapshot isolation for transactions.

/// Iterable for iterating through all vertices of a Storage.
///
/// An instance of this will be usually be wrapped inside VerticesIterable for
/// generic, public use.
class AllVerticesIterable final {
  VertexContainer *vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;
  Config::Items config_;
  const VertexValidator *vertex_validator_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    VertexContainer::iterator it_;

   public:
    Iterator(AllVerticesIterable *self, VertexContainer::iterator it);

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(VertexContainer &vertices_accessor, Transaction *transaction, View view, Indices *indices,
                      Config::Items config, const VertexValidator &vertex_validator)
      : vertices_accessor_(&vertices_accessor),
        transaction_(transaction),
        view_(view),
        indices_(indices),
        config_(config),
        vertex_validator_{&vertex_validator} {}

  Iterator begin() { return {this, vertices_accessor_->begin()}; }
  Iterator end() { return {this, vertices_accessor_->end()}; }
};

/// Generic access to different kinds of vertex iterations.
///
/// This class should be the primary type used by the client code to iterate
/// over vertices inside a Storage instance.
class VerticesIterable final {
  enum class Type { ALL, BY_LABEL, BY_LABEL_PROPERTY };

  Type type_;
  union {
    AllVerticesIterable all_vertices_;
    LabelIndex::Iterable vertices_by_label_;
    LabelPropertyIndex::Iterable vertices_by_label_property_;
  };

 public:
  explicit VerticesIterable(AllVerticesIterable);
  explicit VerticesIterable(LabelIndex::Iterable);
  explicit VerticesIterable(LabelPropertyIndex::Iterable);

  VerticesIterable(const VerticesIterable &) = delete;
  VerticesIterable &operator=(const VerticesIterable &) = delete;

  VerticesIterable(VerticesIterable &&) noexcept;
  VerticesIterable &operator=(VerticesIterable &&) noexcept;

  ~VerticesIterable();

  class Iterator final {
    Type type_;
    union {
      AllVerticesIterable::Iterator all_it_;
      LabelIndex::Iterable::Iterator by_label_it_;
      LabelPropertyIndex::Iterable::Iterator by_label_property_it_;
    };

    void Destroy() noexcept;

   public:
    explicit Iterator(AllVerticesIterable::Iterator);
    explicit Iterator(LabelIndex::Iterable::Iterator);
    explicit Iterator(LabelPropertyIndex::Iterable::Iterator);

    Iterator(const Iterator &);
    Iterator &operator=(const Iterator &);

    Iterator(Iterator &&) noexcept;
    Iterator &operator=(Iterator &&) noexcept;

    ~Iterator();

    VertexAccessor operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const;
    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  Iterator begin();
  Iterator end();
};

/// Structure used to return information about existing indices in the storage.
struct IndicesInfo {
  std::vector<LabelId> label;
  std::vector<std::pair<LabelId, PropertyId>> label_property;
};

/// Structure used to return information about existing schemas in the storage
struct SchemasInfo {
  Schemas::SchemasList schemas;
};

struct SplitInfo {
  uint64_t shard_version;
  PrimaryKey split_point;
};

// If edge properties-on-edges is false then we don't need to send edges but
// only vertices, since they will contain those edges
struct SplitData {
  VertexContainer vertices;
  std::optional<EdgeContainer> edges;
  IndicesInfo indices_info;
  std::map<uint64_t, Transaction> transactions;
};

/// Structure used to return information about the storage.
struct StorageInfo {
  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
};

class Shard final {
 public:
  /// @throw std::system_error
  /// @throw std::bad_alloc
  explicit Shard(LabelId primary_label, PrimaryKey min_primary_key, std::optional<PrimaryKey> max_primary_key,
                 std::vector<SchemaProperty> schema, Config config = Config(),
                 std::unordered_map<uint64_t, std::string> id_to_name = {});

  Shard(const Shard &) = delete;
  Shard(Shard &&) noexcept = delete;
  Shard &operator=(const Shard &) = delete;
  Shard operator=(Shard &&) noexcept = delete;
  ~Shard();

  class Accessor final {
   private:
    friend class Shard;

    Accessor(Shard &shard, Transaction &transaction);

   public:
    /// @throw std::bad_alloc
    ShardResult<VertexAccessor> CreateVertexAndValidate(
        const std::vector<LabelId> &labels, const std::vector<PropertyValue> &primary_properties,
        const std::vector<std::pair<PropertyId, PropertyValue>> &properties);

    std::optional<VertexAccessor> FindVertex(std::vector<PropertyValue> primary_key, View view);

    VerticesIterable Vertices(View view) {
      return VerticesIterable(AllVerticesIterable(shard_->vertices_, transaction_, view, &shard_->indices_,
                                                  shard_->config_.items, shard_->vertex_validator_));
    }

    VerticesIterable Vertices(LabelId label, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view);

    VerticesIterable Vertices(LabelId label, PropertyId property,
                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view);

    /// Return number of all vertices in the database.
    int64_t VertexCount() const { return static_cast<int64_t>(shard_->vertices_.size()); }

    /// Return approximate number of vertices with the given label.
    /// Note that this is always an over-estimate and never an under-estimate.
    int64_t ApproximateVertexCount(LabelId label) const {
      return shard_->indices_.label_index.ApproximateVertexCount(label);
    }

    /// Return number of vertices with the given label and property.
    int64_t VertexCount(LabelId label, PropertyId property) const {
      return shard_->indices_.label_property_index.VertexCount(label, property);
    }

    /// Return number of vertices with the given label and the given
    int64_t VertexCount(LabelId label, PropertyId property, const PropertyValue &value) const {
      return shard_->indices_.label_property_index.VertexCount(label, property, value);
    }

    /// Return number of vertices with the given label and value for
    /// the given property in the range defined by provided upper and lower
    /// bounds.
    int64_t VertexCount(LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
                        const std::optional<utils::Bound<PropertyValue>> &upper) const {
      return shard_->indices_.label_property_index.VertexCount(label, property, lower, upper);
    }

    /// @return Accessor to the deleted vertex if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    ShardResult<std::optional<VertexAccessor>> DeleteVertex(VertexAccessor *vertex);

    /// @return Accessor to the deleted vertex and deleted edges if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    ShardResult<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachDeleteVertex(
        VertexAccessor *vertex);

    /// @throw std::bad_alloc
    ShardResult<EdgeAccessor> CreateEdge(VertexId from_vertex_id, VertexId to_vertex_id, EdgeTypeId edge_type, Gid gid);

    /// Accessor to the deleted edge if a deletion took place, std::nullopt otherwise
    /// @throw std::bad_alloc
    ShardResult<std::optional<EdgeAccessor>> DeleteEdge(VertexId from_vertex_id, VertexId to_vertex_id, Gid edge_id);

    LabelId NameToLabel(std::string_view name) const;

    PropertyId NameToProperty(std::string_view name) const;

    EdgeTypeId NameToEdgeType(std::string_view name) const;

    const std::string &LabelToName(LabelId label) const;

    const std::string &PropertyToName(PropertyId property) const;

    const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

    bool LabelIndexExists(LabelId label) const { return shard_->indices_.label_index.IndexExists(label); }

    bool LabelPropertyIndexExists(LabelId label, PropertyId property) const {
      return shard_->indices_.label_property_index.IndexExists(label, property);
    }

    IndicesInfo ListAllIndices() const {
      return {shard_->indices_.label_index.ListIndices(), shard_->indices_.label_property_index.ListIndices()};
    }

    const SchemaValidator &GetSchemaValidator() const;

    SchemasInfo ListAllSchemas() const { return {shard_->schemas_.ListSchemas()}; }

    void AdvanceCommand();

    void Commit(coordinator::Hlc commit_timestamp);

    /// @throw std::bad_alloc
    void Abort();

   private:
    Shard *shard_;
    Transaction *transaction_;
    Config::Items config_;
  };

  Accessor Access(coordinator::Hlc start_timestamp, std::optional<IsolationLevel> override_isolation_level = {}) {
    return Accessor{*this, GetTransaction(start_timestamp, override_isolation_level.value_or(isolation_level_))};
  }

  LabelId NameToLabel(std::string_view name) const;

  PropertyId NameToProperty(std::string_view name) const;

  EdgeTypeId NameToEdgeType(std::string_view name) const;

  const std::string &LabelToName(LabelId label) const;

  const std::string &PropertyToName(PropertyId property) const;

  const std::string &EdgeTypeToName(EdgeTypeId edge_type) const;

  LabelId PrimaryLabel() const;

  [[nodiscard]] bool IsVertexBelongToShard(const VertexId &vertex_id) const;

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, std::optional<uint64_t> desired_commit_timestamp = {});

  bool DropIndex(LabelId label, PropertyId property, std::optional<uint64_t> desired_commit_timestamp = {});

  IndicesInfo ListAllIndices() const;

  SchemasInfo ListAllSchemas() const;

  const Schemas::Schema *GetSchema(LabelId primary_label) const;

  bool CreateSchema(LabelId primary_label, const std::vector<SchemaProperty> &schemas_types);

  bool DropSchema(LabelId primary_label);

  StorageInfo GetInfo() const;

  void SetIsolationLevel(IsolationLevel isolation_level);

  // Might invalidate accessors
  void CollectGarbage(io::Time current_time);

  void StoreMapping(std::unordered_map<uint64_t, std::string> id_to_name);

  std::optional<SplitInfo> ShouldSplit() const noexcept;

  SplitData PerformSplit(const PrimaryKey &split_key);

 private:
  template <typename TObj>
  requires utils::SameAsAnyOf<TObj, Edge, VertexData>
  void AdjustSplittedDataDeltas(TObj &delta_holder, const std::map<int64_t, Transaction> &transactions) {
    auto *delta_chain = delta_holder.delta;
    Delta *new_delta_chain{nullptr};
    while (delta_chain != nullptr) {
      auto &transaction = transactions.at(delta_chain->command_id);
      // This is the address of corresponding delta
      const auto transaction_delta_it = std::ranges::find_if(
          transaction->deltas, [delta_uuid = delta_chain->uuid](const auto &elem) { return elem.uuid == delta_uuid; });
      // Add this delta to the new chain
      if (new_delta_chain == nullptr) {
        new_delta_chain = &*transaction_delta_it;
      } else {
        new_delta_chain->next = &*transaction_delta_it;
      }
      delta_chain = delta_chain->next;
    }
    delta_holder.delta = new_delta_chain;
  }

  void ScanDeltas(std::set<uint64_t> &collected_transactions_start_id, Delta *delta) const;

  void AlignClonedTransaction(Transaction &cloned_transaction, const Transaction &transaction,
                              std::map<uint64_t, Transaction> &cloned_transactions, VertexContainer &cloned_vertices,
                              EdgeContainer &cloned_edges);

  void AlignClonedTransactions(std::map<uint64_t, Transaction> &cloned_transactions, VertexContainer &cloned_vertices,
                               EdgeContainer &cloned_edges);

  std::map<uint64_t, Transaction> CollectTransactions(const std::set<uint64_t> &collected_transactions_start_id,
                                                      VertexContainer &cloned_vertices, EdgeContainer &cloned_edges);

  VertexContainer CollectVertices(std::set<uint64_t> &collected_transactions_start_id, const PrimaryKey &split_key);

  std::optional<EdgeContainer> CollectEdges(std::set<uint64_t> &collected_transactions_start_id,
                                            const VertexContainer &split_vertices, const PrimaryKey &split_key);

  Transaction &GetTransaction(coordinator::Hlc start_timestamp, IsolationLevel isolation_level);

  uint64_t CommitTimestamp(std::optional<uint64_t> desired_commit_timestamp = {});

  // Main object storage
  NameIdMapper name_id_mapper_;
  LabelId primary_label_;
  // The shard's range is [min, max>
  PrimaryKey min_primary_key_;
  std::optional<PrimaryKey> max_primary_key_;
  VertexContainer vertices_;
  EdgeContainer edges_;
  // Even though the edge count is already kept in the `edges_` SkipList, the
  // list is used only when properties are enabled for edges. Because of that we
  // keep a separate count of edges that is always updated.
  uint64_t edge_count_{0};
  uint64_t shard_version_{0};

  SchemaValidator schema_validator_;
  VertexValidator vertex_validator_;
  Indices indices_;
  Schemas schemas_;

  std::list<Transaction *> committed_transactions_;
  IsolationLevel isolation_level_;

  Config config_;

  // Vertices that are logically deleted but still have to be removed from
  // indices before removing them from the main storage.
  std::list<const PrimaryKey *> deleted_vertices_;

  // Edges that are logically deleted and wait to be removed from the main
  // storage.
  std::list<Gid> deleted_edges_;

  // Holds all of the (in progress, committed and aborted) transactions that are read or write to this shard, but
  // haven't been cleaned up yet
  std::map<uint64_t, std::unique_ptr<Transaction>> start_logical_id_to_transaction_{};
  bool has_any_transaction_aborted_since_last_gc{false};
};

}  // namespace memgraph::storage::v3
