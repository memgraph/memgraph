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

#include <rocksdb/iterator.h>
#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

class SingleVertexIterable final {
  Page *page_;
  uint32_t offset_;
  Storage *storage_;

 public:
  class Iterator final {
    SingleVertexIterable *self_;
    bool last_;
    VertexAccessor vacc;

   public:
    Iterator(SingleVertexIterable *self, bool last)
        : self_{self}, last_{last}, vacc{self_->page_, self_->offset_, self_->storage_} {}

    VertexAccessor const &operator*() const {
      // asm("nop");
      return vacc;
    }

    Iterator &operator++() {
      last_ = true;
      return *this;
    }

    bool operator==(const Iterator &other) const { return self_ == other.self_ && last_ == other.last_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  SingleVertexIterable(Page *page, uint32_t offset, Storage *storage, Transaction *transaction, View view)
      : page_(page), offset_{offset}, storage_(storage) {}

  Iterator begin() { return {this, false}; }
  Iterator end() { return {this, true}; }
};

class AllVerticesIterable final {
  std::unique_ptr<rocksdb::Iterator> itr;
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Storage *storage_;
  Transaction *transaction_;
  View view_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipList<Vertex>::Iterator it_;
    bool last{false};
    uint8_t *chunk_ptr{};
    bool whole{false};

   public:
    Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it);
    Iterator(AllVerticesIterable *self, bool last);

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const {
      if (self_->transaction_->scanned_all_vertices_) {
        return self_ == other.self_ && it_ == other.it_;
      }
      return self_ == other.self_ && last == other.last;
    }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Storage *storage, Transaction *transaction,
                      View view)
      : vertices_accessor_(std::move(vertices_accessor)), storage_(storage), transaction_(transaction), view_(view) {}

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Storage *storage, Transaction *transaction,
                      View view, std::unique_ptr<rocksdb::Iterator> itr)
      : itr{std::move(itr)},
        vertices_accessor_(std::move(vertices_accessor)),
        storage_(storage),
        transaction_(transaction),
        view_(view) {}

  Iterator begin() {
    if (transaction_->scanned_all_vertices_) {
      return {this, vertices_accessor_.begin()};
    }
    itr->SeekToFirst();
    return {this, !itr->Valid()};
  }
  Iterator end() {
    if (transaction_->scanned_all_vertices_) {
      return {this, vertices_accessor_.end()};
    }
    return {this, true};
  }
};

}  // namespace memgraph::storage
