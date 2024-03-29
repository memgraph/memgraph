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

#include "storage/v2/all_vertices_iterable.hpp"

namespace memgraph::storage {

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::Iterator it, utils::SkipList<Vertex>::Iterator end,
                            std::optional<VertexAccessor> *vertex, Storage *storage, Transaction *tx, View view) {
  while (it != end) {
    if (not VertexAccessor::IsVisible(&*it, tx, view)) {
      ++it;
      continue;
    }
    *vertex = VertexAccessor{&*it, storage, tx};
    break;
  }
  return it;
}

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it)
    : self_(self),
      it_(AdvanceToVisibleVertex(it, self->vertices_accessor_.end(), &self->vertex_, self->storage_, self->transaction_,
                                 self->view_)) {}

VertexAccessor const &AllVerticesIterable::Iterator::operator*() const { return *self_->vertex_; }

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(), &self_->vertex_, self_->storage_,
                               self_->transaction_, self_->view_);
  return *this;
}

}  // namespace memgraph::storage
