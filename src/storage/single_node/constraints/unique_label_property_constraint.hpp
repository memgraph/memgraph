/// @file

#pragma once

#include <list>
#include <mutex>

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node/constraints/record.hpp"

namespace tx {
class Snapshot;
};

class VertexAccessor;

namespace storage::constraints {
namespace impl {
struct Pair {
  Pair(gid::Gid gid, const PropertyValue &v, const tx::Transaction &t)
      : value(v), record(gid, t) {}

  PropertyValue value;
  Record record;
};

/// Contains data for every unique constraint rule. It consists of label,
/// property and all property values for that property. For each property value
/// there is a record that contains ids of transactions that created and
/// deleted record with that value.
struct LabelPropertyEntry {
  explicit LabelPropertyEntry(storage::Label l, storage::Property p)
      : label(l), property(p) {}

  storage::Label label;
  storage::Property property;
  std::list<Pair> version_pairs;
};
}  // namespace impl

struct LabelProperty {
  // This struct is used by ListConstraints method in order to avoid using
  // std::pair or something like that.
  storage::Label label;
  storage::Property property;
};

/// UniqueLabelPropertyConstraint contains all unique constraints defined by
/// both label and property. To create or delete unique constraint, caller
/// must ensure that there are no other transactions running in parallel.
/// Additionally, for adding unique constraint caller must first call
/// AddConstraint to create unique constraint and then call UpdateOnAddLabel or
/// UpdateOnAddProperty for every existing Vertex. If there is a unique
/// constraint violation then caller must manually handle that by catching
/// exception and calling RemoveConstraint method. This is needed to ensure
/// logical correctness of transactions. Once created, client uses UpdateOn
/// methods to notify UniqueLabelPropertyConstraint about changes. In case of
/// violation UpdateOn methods throw IndexConstraintViolationException
/// exception. Methods can also throw SerializationError. This class is thread
/// safe.
class UniqueLabelPropertyConstraint {
 public:
  UniqueLabelPropertyConstraint() = default;
  UniqueLabelPropertyConstraint(const UniqueLabelPropertyConstraint &) = delete;
  UniqueLabelPropertyConstraint(UniqueLabelPropertyConstraint &&) = delete;
  UniqueLabelPropertyConstraint &operator=(
      const UniqueLabelPropertyConstraint &) = delete;
  UniqueLabelPropertyConstraint &operator=(UniqueLabelPropertyConstraint &&) =
      delete;

  /// Add new unique constraint, if constraint already exists this method does
  /// nothing. This method doesn't check if any of the existing vertices breaks
  /// this constraint. Caller must do that instead. Caller must also ensure that
  /// no other transaction is running in parallel.
  void AddConstraint(storage::Label label, storage::Property property,
                     const tx::Transaction &t);

  /// Removes existing unique constraint, if the constraint doesn't exist this
  /// method does nothing. Caller must ensure that no other transaction is
  /// running in parallel.
  void RemoveConstraint(storage::Label label, storage::Property property);

  /// Checks whether given unique constraint is visible.
  bool Exists(storage::Label label, storage::Property property) const;

  /// Returns list of unique constraints.
  std::vector<LabelProperty> ListConstraints() const;

  /// Updates unique constraint versions.
  ///
  /// @throws IndexConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddLabel(storage::Label label, const VertexAccessor &accessor,
                        const tx::Transaction &t);

  /// Updates unique constraint versions.
  ///
  /// @throws SerializationError
  void UpdateOnRemoveLabel(storage::Label label, const VertexAccessor &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions.
  ///
  /// @throws IndexConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddProperty(storage::Property property,
                           const PropertyValue &value,
                           const VertexAccessor &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions.
  ///
  /// @throws SerializationError
  void UpdateOnRemoveProperty(storage::Property property,
                              const PropertyValue &value,
                              const VertexAccessor &accessor,
                              const tx::Transaction &t);

  /// Removes records that are no longer visible.
  void Refresh(const tx::Snapshot &snapshot, const tx::Engine &engine);

 private:
  std::mutex lock_;

  std::list<impl::LabelPropertyEntry> constraints_;
};
}  // namespace storage::constraints
