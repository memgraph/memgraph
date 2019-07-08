#pragma once

#include <optional>

#include "storage/v2/edge.hpp"

#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

struct Vertex;
class VertexAccessor;
class Storage;

class EdgeAccessor final {
 private:
  friend class Storage;

 public:
  EdgeAccessor(Edge *edge, uint64_t edge_type, Vertex *from_vertex,
               Vertex *to_vertex, Transaction *transaction)
      : edge_(edge),
        edge_type_(edge_type),
        from_vertex_(from_vertex),
        to_vertex_(to_vertex),
        transaction_(transaction) {}

  VertexAccessor FromVertex();

  VertexAccessor ToVertex();

  uint64_t EdgeType() const { return edge_type_; }

  Gid Gid() const { return edge_->gid; }

  bool operator==(const EdgeAccessor &other) const {
    return edge_ == other.edge_ && transaction_ == other.transaction_;
  }
  bool operator!=(const EdgeAccessor &other) const { return !(*this == other); }

 private:
  Edge *edge_;
  uint64_t edge_type_;
  Vertex *from_vertex_;
  Vertex *to_vertex_;
  Transaction *transaction_;
};

}  // namespace storage
