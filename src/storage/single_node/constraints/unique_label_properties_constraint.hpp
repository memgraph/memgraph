/// @file

#pragma once

#include <list>
#include <mutex>

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node/constraints/record.hpp"

namespace tx {
class Snapshot;
};  // namespace tx

class VertexAccessor;

namespace storage::constraints {
namespace impl {
struct LabelPropertyPair {
  LabelPropertyPair(gid::Gid gid, const std::vector<PropertyValue> &v,
       const tx::Transaction &t)
      : values(v), record(gid, t) {}

  std::vector<PropertyValue> values;
  Record record;
};

struct LabelPropertiesEntry {
  LabelPropertiesEntry(storage::Label l, const std::vector<storage::Property> &p)
      : label(l), properties(p) {}

  storage::Label label;
  std::vector<storage::Property> properties;
  std::list<LabelPropertyPair> version_pairs;
};
}  // namespace impl

struct LabelProperties {
  // This struct is used by ListConstraints method in order to avoid using
  // std::pair or something like that.
  storage::Label label;
  std::vector<storage::Property> properties;
};

/// UniqueLabelPropertiesConstraint contains all unique constraints defined by
/// both label and set of properties. To create or delete unique constraint, caller
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
class UniqueLabelPropertiesConstraint {
 public:
  UniqueLabelPropertiesConstraint() = default;
  UniqueLabelPropertiesConstraint(const UniqueLabelPropertiesConstraint &) =
      delete;
  UniqueLabelPropertiesConstraint(UniqueLabelPropertiesConstraint &&) = delete;
  UniqueLabelPropertiesConstraint &operator=(
      const UniqueLabelPropertiesConstraint &) = delete;
  UniqueLabelPropertiesConstraint &operator=(
      UniqueLabelPropertiesConstraint &&) = delete;

  /// Add new unique constraint, if constraint already exists this method does
  /// nothing. This method doesn't check if any of the existing vertices breaks
  /// this constraint. Caller must do that instead. Caller must also ensure that
  /// no other transaction is running in parallel.
  void AddConstraint(storage::Label label,
                     const std::vector<storage::Property> &properties,
                     const tx::Transaction &t);

  /// Removes existing unique constraint, if the constraint doesn't exist this
  /// method does nothing. Caller must ensure that no other transaction is
  /// running in parallel.
  void RemoveConstraint(storage::Label label,
                        const std::vector<storage::Property> &properties);

  /// Checks whether given unique constraint is visible.
  bool Exists(storage::Label label,
              const std::vector<storage::Property> &properties) const;

  /// Returns list of unique constraints.
  std::vector<LabelProperties> ListConstraints() const;

  /// Updates unique constraint versions when adding label.
  /// @param label - label that was added
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws IndexConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddLabel(storage::Label label, const VertexAccessor &accessor,
                        const tx::Transaction &t);

  /// Updates unique constraint versions when removing label.
  /// @param label - label that was removed
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws SerializationError
  void UpdateOnRemoveLabel(storage::Label label, const VertexAccessor &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions when adding property.
  /// @param property - property that was added
  /// @param value - property value that was added
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws IndexConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddProperty(storage::Property property,
                           const PropertyValue &value,
                           const VertexAccessor &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions when removing property.
  /// @param property - property that was removed
  /// @param value - property value that was removed
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
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

  std::list<impl::LabelPropertiesEntry> constraints_;
};
}  // namespace storage::constraints
