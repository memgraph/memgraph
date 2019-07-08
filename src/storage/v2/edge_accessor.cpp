#include "storage/v2/edge_accessor.hpp"

#include <memory>

#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace storage {

VertexAccessor EdgeAccessor::FromVertex() {
  return VertexAccessor{from_vertex_, transaction_};
}

VertexAccessor EdgeAccessor::ToVertex() {
  return VertexAccessor{to_vertex_, transaction_};
}

}  // namespace storage
