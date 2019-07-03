#pragma once

#include <optional>

#include "storage/v2/vertex.hpp"

#include "storage/v2/result.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace storage {

class VertexAccessor final {
 private:
  VertexAccessor(Vertex *vertex, Transaction *transaction)
      : vertex_(vertex), transaction_(transaction) {}

 public:
  static std::optional<VertexAccessor> Create(Vertex *vertex,
                                              Transaction *transaction,
                                              View view);

  Result<bool> Delete();

  Result<bool> AddLabel(uint64_t label);

  Result<bool> RemoveLabel(uint64_t label);

  Result<bool> HasLabel(uint64_t label, View view);

  Result<std::vector<uint64_t>> Labels(View view);

  Result<bool> SetProperty(uint64_t property, const PropertyValue &value);

  Result<PropertyValue> GetProperty(uint64_t property, View view);

  Result<std::unordered_map<uint64_t, PropertyValue>> Properties(View view);

  Gid Gid() const { return vertex_->gid; }

 private:
  Vertex *vertex_;
  Transaction *transaction_;
};

}  // namespace storage
