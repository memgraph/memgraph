#pragma once

#include <optional>

#include "storage/v2/edge.hpp"
#include "storage/v2/edge_ref.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

struct Vertex;
class VertexAccessor;
struct Indices;
struct Constraints;

class EdgeAccessor final {
 private:
  friend class Storage;

 public:
  EdgeAccessor(EdgeRef edge, EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Transaction *transaction,
               Indices *indices, Constraints *constraints, Config::Items config)
      : edge_(edge),
        edge_type_(edge_type),
        from_vertex_(from_vertex),
        to_vertex_(to_vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config) {}

  VertexAccessor FromVertex() const;

  VertexAccessor ToVertex() const;

  EdgeTypeId EdgeType() const { return edge_type_; }

  /// Set a property value and return `true` if insertion took place.
  /// `false` is returned if assignment took place.
  /// @throw std::bad_alloc
  Result<bool> SetProperty(PropertyId property, const PropertyValue &value);

  /// Remove all properties and return `true` if any removal took place.
  /// `false` is returned if there were no properties to remove.
  /// @throw std::bad_alloc
  Result<bool> ClearProperties();

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const;

  Gid Gid() const {
    if (config_.properties_on_edges) {
      return edge_.ptr->gid;
    } else {
      return edge_.gid;
    }
  }

  bool IsCycle() const { return from_vertex_ == to_vertex_; }

  bool operator==(const EdgeAccessor &other) const {
    return edge_ == other.edge_ && transaction_ == other.transaction_;
  }
  bool operator!=(const EdgeAccessor &other) const { return !(*this == other); }

 private:
  EdgeRef edge_;
  EdgeTypeId edge_type_;
  Vertex *from_vertex_;
  Vertex *to_vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

}  // namespace storage

namespace std {
template <>
struct hash<storage::EdgeAccessor> {
  size_t operator()(const storage::EdgeAccessor &e) const { return e.Gid().AsUint(); }
};
}  // namespace std
