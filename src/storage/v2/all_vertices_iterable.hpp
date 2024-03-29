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

#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Storage *storage_;
  Transaction *transaction_;
  View view_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipList<Vertex>::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it);

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Storage *storage, Transaction *transaction,
                      View view)
      : vertices_accessor_(std::move(vertices_accessor)), storage_(storage), transaction_(transaction), view_(view) {}

  Iterator begin() { return {this, vertices_accessor_.begin()}; }
  Iterator end() { return {this, vertices_accessor_.end()}; }
};

}  // namespace memgraph::storage
