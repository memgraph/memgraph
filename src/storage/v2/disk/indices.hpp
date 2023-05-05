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

#include <optional>
#include <tuple>
#include <utility>
#include "storage/v2/config.hpp"
#include "storage/v2/constraints.hpp"
#include "storage/v2/disk/disk_vertex.hpp"
#include "storage/v2/disk/rocksdb_storage.hpp"
#include "storage/v2/disk/vertices_iterable.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/exceptions.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

struct DiskIndices;

/// Immovable implementation of LabelDiskIndex for on-disk storage.
class LabelDiskIndex {
 public:
  explicit LabelDiskIndex(DiskIndices *indices, Config config);
  LabelDiskIndex() = delete;

  LabelDiskIndex(const LabelDiskIndex &) = delete;
  LabelDiskIndex &operator=(const LabelDiskIndex &) = delete;

  LabelDiskIndex(LabelDiskIndex &&) = delete;
  LabelDiskIndex &operator=(LabelDiskIndex &&) = delete;

  ~LabelDiskIndex() = default;

  /// Stores all vertices in the RocksDB instances. Vertices are intentionally transferred as pure strings to avoid
  /// unnecessary deserialization and serialization.
  /// @tparam vertices is a vector of tuples where each tuple contains key, value and the timestamp it has been saved
  /// with
  bool CreateIndex(LabelId label, const std::vector<std::tuple<std::string, std::string, uint64_t>> &vertices);

  bool DropIndex(LabelId label);

  bool IndexExists(LabelId label) const;

  std::vector<LabelId> ListIndices() const;

  int64_t ApproximateVertexCount(LabelId label) const;

  /// Clear all indexed vertices from the disk
  void Clear();

  /// TODO: Maybe we can remove completely interaction with garbage collector
  void RunGC();

  class Iterable {
   public:
    Iterable(std::vector<std::unique_ptr<VertexAccessor>> index_accessor, LabelId label, View view,
             Transaction *transaction, DiskIndices *indices, Constraints *constraints, Config config)
        : index_accessor_(std::move(index_accessor)),
          label_(label),
          view_(view),
          transaction_(transaction),
          indices_(indices),
          constraints_(constraints),
          config_(config) {}

    class Iterator {
     public:
      Iterator(Iterable *self, std::vector<std::unique_ptr<VertexAccessor>>::iterator index_iterator)
          : self_(self), index_iterator_(index_iterator) {}
      Iterator(const Iterator &other)
          : self_(other.self_), index_iterator_(other.index_iterator_), current_vertex_(other.current_vertex_) {
        /// TODO: (andi) Advance until valid, how to do
      }

      Iterator(Iterator &&other) = default;
      ~Iterator() = default;

      VertexAccessor *operator*() const { return index_iterator_->get(); }

      bool operator==(const Iterator &other) const { return index_iterator_ == other.index_iterator_; }
      bool operator!=(const Iterator &other) const { return index_iterator_ != other.index_iterator_; }

      Iterator &operator++();

     private:
      Iterable *self_;
      std::vector<std::unique_ptr<VertexAccessor>>::iterator index_iterator_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return Iterator(this, index_accessor_.begin()); }
    Iterator end() { return Iterator(this, index_accessor_.end()); }

   private:
    std::vector<std::unique_ptr<VertexAccessor>> index_accessor_;
    LabelId label_;
    View view_;
    Transaction *transaction_;
    DiskIndices *indices_;
    Constraints *constraints_;
    Config config_;
  };

  /// TODO(andi): If there are no other usages of constaints_ and config_ maybe we can remove
  /// them from here
  Iterable Vertices(LabelId label, View view, Transaction *transaction);

 private:
  std::vector<LabelId> index_;
  DiskIndices *indices_;
  Config config_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

/// Immovable implementation of LabelPropertyDiskIndex for on-disk storage.
class LabelPropertyDiskIndex {
 public:
  explicit LabelPropertyDiskIndex(DiskIndices *indices, Config config);
  LabelPropertyDiskIndex() = delete;

  LabelPropertyDiskIndex(const LabelPropertyDiskIndex &) = delete;
  LabelPropertyDiskIndex &operator=(const LabelPropertyDiskIndex &) = delete;

  LabelPropertyDiskIndex(LabelPropertyDiskIndex &&) = delete;
  LabelPropertyDiskIndex &operator=(LabelPropertyDiskIndex &&) = delete;

  ~LabelPropertyDiskIndex() = default;

  /// TODO(andi): If there are no other usages of constaints_ and config_ maybe we can remove
  /// them from here
  // VerticesIterable Vertices(LabelId label, PropertyId property,
  //                           const std::optional<utils::Bound<PropertyValue>> &lower_bound,
  //                           const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
  //                           Transaction *transaction) {
  //   utils::SkipList<Vertex> vertices;
  //   // return VerticesIterable(AllMemoryVerticesIterable(vertices.access(), transaction, view, indices_,
  //   constraints_,
  //   // config_));
  //   throw utils::NotYetImplemented("LabelPropertyIndex::Vertices");
  // }

  bool CreateIndex(LabelId label, PropertyId property);

  bool DropIndex(LabelId label, PropertyId property);

  bool IndexExists(LabelId label, PropertyId property) const;

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const;

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const;

  /// Clear all indexed vertices from the disk
  void Clear();

  /// TODO: Maybe we can remove completely interaction with garbage collector
  void RunGC();

 private:
  std::vector<std::pair<LabelId, PropertyId>> index_;
  DiskIndices *indices_;
  Config config_;
  std::unique_ptr<RocksDBStorage> kvstore_;
};

/// Immovable implementation of disk indices. Stored with the help of RocksDB in LSM trees.
struct DiskIndices {
  explicit DiskIndices(Config config) : label_index(this, config), label_property_index(this, config) {}
  DiskIndices() = delete;

  DiskIndices(const DiskIndices &) = delete;
  DiskIndices &operator=(const DiskIndices &) = delete;

  DiskIndices(DiskIndices &&) = delete;
  DiskIndices &operator=(DiskIndices &&) = delete;

  ~DiskIndices() = default;

  LabelDiskIndex label_index;
  LabelPropertyDiskIndex label_property_index;
};

void RemoveObsoleteEntries(DiskIndices *indices, uint64_t oldest_active_start_timestamp);

}  // namespace memgraph::storage
