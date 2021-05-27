#pragma once

#include <optional>

#include "storage/v2/vertex.hpp"

#include "storage/v2/config.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

class EdgeAccessor;
class Storage;
struct Indices;
struct Constraints;

class VertexAccessor final {
 private:
  friend class Storage;

 public:
  VertexAccessor(Vertex *vertex, Transaction *transaction, Indices *indices, Constraints *constraints,
                 Config::Items config, bool for_deleted = false)
      : vertex_(vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config),
        for_deleted_(for_deleted) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex, Transaction *transaction, Indices *indices,
                                              Constraints *constraints, Config::Items config, View view);

  /// @return true if the object is visible from the current transaction
  bool IsVisible(View view) const;

  /// Add a label and return `true` if insertion took place.
  /// `false` is returned if the label already existed.
  /// @throw std::bad_alloc
  Result<bool> AddLabel(LabelId label);

  /// Remove a label and return `true` if deletion took place.
  /// `false` is returned if the vertex did not have a label already.
  /// @throw std::bad_alloc
  Result<bool> RemoveLabel(LabelId label);

  Result<bool> HasLabel(LabelId label, View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<LabelId>> Labels(View view) const;

  /// Set a property value and return the old value.
  /// @throw std::bad_alloc
  Result<PropertyValue> SetProperty(PropertyId property, const PropertyValue &value);

  /// Remove all properties and return the values of the removed properties.
  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> ClearProperties();

  /// @throw std::bad_alloc
  Result<PropertyValue> GetProperty(PropertyId property, View view) const;

  /// @throw std::bad_alloc
  Result<std::map<PropertyId, PropertyValue>> Properties(View view) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> InEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                            const VertexAccessor *destination = nullptr) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> OutEdges(View view, const std::vector<EdgeTypeId> &edge_types = {},
                                             const VertexAccessor *destination = nullptr) const;

  Result<size_t> InDegree(View view) const;

  Result<size_t> OutDegree(View view) const;

  Gid Gid() const { return vertex_->gid; }

  bool operator==(const VertexAccessor &other) const {
    return vertex_ == other.vertex_ && transaction_ == other.transaction_;
  }
  bool operator!=(const VertexAccessor &other) const { return !(*this == other); }

 private:
  Vertex *vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;

  // if the accessor was created for a deleted vertex.
  // Accessor behaves differently for some methods based on this
  // flag.
  // E.g. If this field is set to true, GetProperty will return the property of the node
  // even though the node is deleted.
  // All the write operations, and operators used for traversal (e.g. InEdges) will still
  // return an error if it's called for a deleted vertex.
  bool for_deleted_{false};
};

}  // namespace storage

namespace std {
template <>
struct hash<storage::VertexAccessor> {
  size_t operator()(const storage::VertexAccessor &v) const noexcept { return v.Gid().AsUint(); }
};
}  // namespace std
