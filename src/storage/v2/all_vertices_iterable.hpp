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

#include <cstdint>
#include <limits>

#include "storage/v2/id_types.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

class Storage;

inline constexpr Gid kIteratorNoGidUpperBound = Gid::FromUint(std::numeric_limits<uint64_t>::max());

class AllVerticesIterable final {
  utils::SkipListDb<Vertex>::Accessor vertices_accessor_;
  Storage *storage_;
  Transaction *transaction_;
  View view_;
  // exclusive upper bound; vertices created after this iterable opened are out of scope.
  Gid max_gid_;
  std::optional<VertexAccessor> vertex_;

 public:
  class Iterator final {
    AllVerticesIterable *self_;
    utils::SkipListDb<Vertex>::Iterator it_;

   public:
    Iterator(AllVerticesIterable *self, utils::SkipListDb<Vertex>::Iterator it);

    VertexAccessor const &operator*() const;

    Iterator &operator++();

    bool operator==(const Iterator &other) const { return self_ == other.self_ && it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }
  };

  AllVerticesIterable(utils::SkipListDb<Vertex>::Accessor vertices_accessor, Storage *storage, Transaction *transaction,
                      View view, Gid max_gid)
      : vertices_accessor_(std::move(vertices_accessor)),
        storage_(storage),
        transaction_(transaction),
        view_(view),
        max_gid_(max_gid) {}

  Iterator begin() { return {this, vertices_accessor_.begin()}; }

  Iterator end() { return {this, vertices_accessor_.end()}; }
};

}  // namespace memgraph::storage
