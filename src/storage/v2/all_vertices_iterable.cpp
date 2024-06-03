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
#include "utils/rocksdb_serialization.hpp"

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

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self, bool last) : self_(self), last(last) {}

VertexAccessor const AllVerticesIterable::Iterator::operator*() const {
  //
  // return *self_->vertex_;

  const auto gid = utils::ExtractGidFromKey(0, self_->itr->key().ToStringView());
  auto labels_id = utils::DeserializeLabelsFromMainDiskStorage(self_->itr->key().ToStringView());
  auto properties = utils::DeserializePropertiesFromMainDiskStorage(self_->itr->value().ToStringView());

  Delta *delta{};
  self_->transaction_->EnsureCommitTimestampExists();
  if (self_->transaction_->deltas.empty()) {
    delta = &self_->transaction_->deltas.emplace_back(Delta::DeleteDeserializedObjectTag(), 0,
                                                      self_->itr->key().ToStringView().data());
  } else {
    delta = &self_->transaction_->deltas.front();
  }

  // auto [it, inserted] = self_->vertices_accessor_.insert(Vertex{gid, delta});
  // MG_ASSERT(inserted, "The vertex must be inserted here!");
  // MG_ASSERT(it != self_->vertices_accessor_.end(), "Invalid Vertex accessor!");
  // it->labels = std::move(labels_id);
  // it->properties = std::move(properties);
  // delta->prev.Set(&*it);
  auto *v = new Vertex{gid, delta};
  v->labels = std::move(labels_id);
  v->properties = std::move(properties);
  delta->prev.Set(&*v);
  return {&*v, self_->storage_, self_->transaction_};
}

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  self_->itr->Next();
  last = !self_->itr->Valid();
  return *this;
  // ++it_;
  // it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(), &self_->vertex_, self_->storage_,
  //                              self_->transaction_, self_->view_);
  // return *this;
}

}  // namespace memgraph::storage
