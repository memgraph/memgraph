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

#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class AllVerticesIterable final {
  utils::SkipList<Vertex>::Accessor vertices_accessor_;
  Transaction *transaction_;
  View view_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
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

  AllVerticesIterable(utils::SkipList<Vertex>::Accessor vertices_accessor, Transaction *transaction, View view,
                      Indices *indices, Constraints *constraints, Config::Items config)
      : vertices_accessor_(std::move(vertices_accessor)),
        transaction_(transaction),
        view_(view),
        indices_(indices),
        constraints_(constraints),
        config_(config) {}

  Iterator begin() { return {this, vertices_accessor_.begin()}; }
  Iterator end() { return {this, vertices_accessor_.end()}; }
};

}  // namespace memgraph::storage
