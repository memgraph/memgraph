/// @file

#pragma once

#include <list>
#include <mutex>

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/common/constraints/record.hpp"

namespace tx {
class Snapshot;
};  // namespace tx

class Vertex;

template <typename TRecord>
class RecordAccessor;

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
  LabelPropertiesEntry(storage::Label l,
                       const std::vector<storage::Property> &p)
      : label(l), properties(p) {}

  storage::Label label;
  std::vector<storage::Property> properties;
  std::list<LabelPropertyPair> version_pairs;
};
}  // namespace impl

struct ConstraintEntry {
  // This struct is used by ListConstraints method in order to avoid using
  // std::pair or something like that.
  storage::Label label;
  std::vector<storage::Property> properties;
};

/// UniqueConstraints contains all unique constraints defined by both label and
/// a set of properties. To create or delete unique constraint, caller must
/// ensure that there are no other transactions running in parallel.
/// Additionally, for adding unique constraint caller must first call
/// AddConstraint to create unique constraint and then call Update for every
/// existing Vertex. If there is a unique constraint violation, the caller must
/// manually handle that by catching exceptions and calling RemoveConstraint
/// method. This is needed to ensure logical correctness of transactions. Once
/// created, client uses UpdateOn* methods to notify UniqueConstraint about
/// changes. In case of violation UpdateOn* methods throw
/// ConstraintViolationException exception. Methods can also throw
/// SerializationError. This class is thread safe.
class UniqueConstraints {
 public:
  UniqueConstraints() = default;
  UniqueConstraints(const UniqueConstraints &) = delete;
  UniqueConstraints(UniqueConstraints &&) = delete;
  UniqueConstraints &operator=(const UniqueConstraints &) = delete;
  UniqueConstraints &operator=(UniqueConstraints &&) = delete;

  ~UniqueConstraints() = default;

  /// Add new unique constraint, if constraint already exists this method does
  /// nothing. This method doesn't check if any of the existing vertices breaks
  /// this constraint. Caller must do that instead. Caller must also ensure that
  /// no other transaction is running in parallel.
  ///
  /// @return true if the constraint doesn't exists and was added.
  bool AddConstraint(const ConstraintEntry &entry);

  /// Removes existing unique constraint, if the constraint doesn't exist this
  /// method does nothing. Caller must ensure that no other transaction is
  /// running in parallel.
  ///
  /// @return true if the constraint existed and was removed.
  bool RemoveConstraint(const ConstraintEntry &entry);

  /// Checks whether given unique constraint is visible.
  bool Exists(storage::Label label,
              const std::vector<storage::Property> &properties) const;

  /// Returns list of unique constraints.
  std::vector<ConstraintEntry> ListConstraints() const;

  /// Updates unique constraint versions when adding new constraint rule.
  ///
  /// @throws ConstraintViolationException
  /// @throws SerializationError
  void Update(const RecordAccessor<Vertex> &accessor, const tx::Transaction &t);

  /// Updates unique constraint versions when adding label.
  /// @param label - label that was added
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws ConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddLabel(storage::Label label,
                        const RecordAccessor<Vertex> &accessor,
                        const tx::Transaction &t);

  /// Updates unique constraint versions when removing label.
  /// @param label - label that was removed
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws SerializationError
  void UpdateOnRemoveLabel(storage::Label label,
                           const RecordAccessor<Vertex> &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions when adding property.
  /// @param property - property that was added
  /// @param previous_value - previous value of the property
  /// @param new_value - new value of the property
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws ConstraintViolationException
  /// @throws SerializationError
  void UpdateOnAddProperty(storage::Property property,
                           const PropertyValue &previous_value,
                           const PropertyValue &new_value,
                           const RecordAccessor<Vertex> &accessor,
                           const tx::Transaction &t);

  /// Updates unique constraint versions when removing property.
  /// @param property - property that was removed
  /// @param previous_value - previous value of the property
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws SerializationError
  void UpdateOnRemoveProperty(storage::Property property,
                              const PropertyValue &previous_value,
                              const RecordAccessor<Vertex> &accessor,
                              const tx::Transaction &t);

  /// Updates unique constraint versions when removing a vertex.
  /// @param accessor - accessor that was updated
  /// @param t - current transaction
  ///
  /// @throws SerializationError
  void UpdateOnRemoveVertex(const RecordAccessor<Vertex> &accessor,
                            const tx::Transaction &t);

  /// Removes records that are no longer visible.
  /// @param snapshot - the GC snapshot.
  /// @param engine   - current transaction engine.
  void Refresh(const tx::Snapshot &snapshot, const tx::Engine &engine);

 private:
  std::mutex lock_;

  std::list<impl::LabelPropertiesEntry> constraints_;
};
}  // namespace storage::constraints
