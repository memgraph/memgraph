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
  VertexAccessor(Vertex *vertex, Transaction *transaction, Indices *indices,
                 Constraints *constraints, Config::Items config)
      : vertex_(vertex),
        transaction_(transaction),
        indices_(indices),
        constraints_(constraints),
        config_(config) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex,
                                              Transaction *transaction,
                                              Indices *indices,
                                              Constraints *constraints,
                                              Config::Items config, View view);

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

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> InEdges(
      View view, const std::vector<EdgeTypeId> &edge_types = {},
      const VertexAccessor *destination = nullptr) const;

  /// @throw std::bad_alloc
  /// @throw std::length_error if the resulting vector exceeds
  ///        std::vector::max_size().
  Result<std::vector<EdgeAccessor>> OutEdges(
      View view, const std::vector<EdgeTypeId> &edge_types = {},
      const VertexAccessor *destination = nullptr) const;

  Result<size_t> InDegree(View view) const;

  Result<size_t> OutDegree(View view) const;

  Gid Gid() const { return vertex_->gid; }

  bool operator==(const VertexAccessor &other) const {
    return vertex_ == other.vertex_ && transaction_ == other.transaction_;
  }
  bool operator!=(const VertexAccessor &other) const {
    return !(*this == other);
  }

 private:
  Vertex *vertex_;
  Transaction *transaction_;
  Indices *indices_;
  Constraints *constraints_;
  Config::Items config_;
};

}  // namespace storage

namespace std {
template <>
struct hash<storage::VertexAccessor> {
  size_t operator()(const storage::VertexAccessor &v) const noexcept {
    return v.Gid().AsUint();
  }
};
}  // namespace std
