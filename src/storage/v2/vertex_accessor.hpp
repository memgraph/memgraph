#pragma once

#include <optional>

#include "storage/v2/vertex.hpp"

#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

class EdgeAccessor;
class Storage;

class VertexAccessor final {
 private:
  friend class Storage;

 public:
  VertexAccessor(Vertex *vertex, Transaction *transaction)
      : vertex_(vertex), transaction_(transaction) {}

  static std::optional<VertexAccessor> Create(Vertex *vertex,
                                              Transaction *transaction,
                                              View view);

  Result<bool> AddLabel(LabelId label);

  Result<bool> RemoveLabel(LabelId label);

  Result<bool> HasLabel(LabelId label, View view);

  Result<std::vector<LabelId>> Labels(View view);

  Result<bool> SetProperty(PropertyId property, const PropertyValue &value);

  Result<PropertyValue> GetProperty(PropertyId property, View view);

  Result<std::map<PropertyId, PropertyValue>> Properties(View view);

  Result<std::vector<EdgeAccessor>> InEdges(
      const std::vector<EdgeTypeId> &edge_types, View view);

  Result<std::vector<EdgeAccessor>> OutEdges(
      const std::vector<EdgeTypeId> &edge_types, View view);

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
};

}  // namespace storage

namespace std {
template <>
struct hash<storage::VertexAccessor> {
  size_t operator()(const storage::VertexAccessor &v) const {
    return v.Gid().AsUint();
  }
};
}  // namespace std
