#pragma once

#include "database/graph_db.hpp"
//#include "database/graph_db_accessor.hpp"
#include "mvcc/version_list.hpp"
#include "storage/property_value.hpp"

#include "storage/property_value_store.hpp"

class GraphDbAccessor;

/**
 * An accessor to a database record (an Edge or a Vertex).
 *
 * Exposes view and update functions to the client programmer.
 * Assumes responsibility of doing all the relevant book-keeping
 * (such as index updates etc).
 *
 * @tparam TRecord Type of record (MVCC Version) of the accessor.
 */
template <typename TRecord>
class RecordAccessor {
 public:
  /**
   * The GraphDbAccessor is friend to this accessor so it can
   * operate on it's data (mvcc version-list and the record itself).
   * This is legitemate because GraphDbAccessor creates RecordAccessors
   * and is semantically their parent/owner. It is necessary because
   * the GraphDbAccessor handles insertions and deletions, and these
   * operations modify data intensively.
   */
  friend GraphDbAccessor;

  /**
   * @param vlist MVCC record that this accessor wraps.
   * @param db_accessor The DB accessor that "owns" this record accessor.
   */
  RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                 GraphDbAccessor &db_accessor);

  // this class is default copyable, movable and assignable
  RecordAccessor(const RecordAccessor &other) = default;
  RecordAccessor(RecordAccessor &&other) = default;
  RecordAccessor &operator=(const RecordAccessor &other) = default;
  RecordAccessor &operator=(RecordAccessor &&other) = default;

  /**
   * Gets the property for the given key.
   * @param key
   * @return
   */
  const PropertyValue &PropsAt(GraphDbTypes::Property key) const;

  /**
   * Sets a value on the record for the given property.
   *
   * @tparam TValue Type of the value being set.
   * @param key Property key.
   * @param value The value to set.
   */
  template <typename TValue>
  void PropsSet(GraphDbTypes::Property key, TValue value) {
    update().properties_.set(key, value);
  }

  /**
   * Erases the property for the given key.
   *
   * @param key
   * @return
   */
  size_t PropsErase(GraphDbTypes::Property key);

  /**
   * Removes all the properties from this record.
   */
  void PropsClear();

  /**
   * Returns the properties of this record.
   * @return
   */
  const PropertyValueStore<GraphDbTypes::Property> &Properties() const;

  void PropertiesAccept(std::function<void(const GraphDbTypes::Property key,
                                           const PropertyValue &prop)>
                            handler,
                        std::function<void()> finish = {}) const;

  /**
   * This should be used with care as it's comparing vlist_ pointer records and
   * not actual values inside RecordAccessors.
   */
  friend bool operator<(const RecordAccessor &a, const RecordAccessor &b) {
    debug_assert(a.db_accessor_ == b.db_accessor_,
                 "Not in the same transaction.");  // assume the same
                                                   // db_accessor / transaction
    return a.vlist_ < b.vlist_;
  }

  friend bool operator==(const RecordAccessor &a, const RecordAccessor &b) {
    debug_assert(a.db_accessor_ == b.db_accessor_,
                 "Not in the same transaction.");  // assume the same
                                                   // db_accessor / transaction
    return a.vlist_ == b.vlist_;
  }

  friend bool operator!=(const RecordAccessor &a, const RecordAccessor &b) {
    debug_assert(a.db_accessor_ == b.db_accessor_,
                 "Not in the same transaction.");  // assume the same
                                                   // db_accessor / transaction
    return !(a == b);
  }

  /**
   * Returns a GraphDB accessor of this record accessor.
   *
   * @return See above.
   */
  GraphDbAccessor &db_accessor() const;

  /**
   * Returns a temporary ID of the record stored in this accessor.
   *
   * This function returns a number that represents the current memory
   * location where the record is stored. That number is used only as an
   * identification for the database snapshotter. The snapshotter needs an
   * ID so that when the database is saved to disk that it can be successfully
   * reconstructed.
   * IMPORTANT: The ID is valid for identifying graph elements observed in
   * the same transaction. It is not valid for comparing graph elements
   * observed in different transactions.
   *
   * @return See above.
   */
  uint64_t temporary_id() const;

  /*
   * Switches this record accessor to use the latest
   * version (visible to the current transaction+command).
   *
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchNew();

  /**
   * Switches this record accessor to use the old
   * (not updated) version visible to the current transaction+command.
   *
   * It is not legal to call this function on a Vertex/Edge that
   * was created by the current transaction+command.
   *
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchOld();

  /**
   * Reconstructs the internal state of the record
   * accessor so it uses the versions appropriate
   * to this transaction+command.
   *
   * TODO consider what it does after delete+advance_command
   */
  void Reconstruct();

 protected:
  /**
   * Ensures there is an updateable version of the record
   * in the version_list, and that the `new_` pointer
   * points to it. Returns a reference to that version.
   *
   * @return See above.
   */
  TRecord &update();

  /**
   * Returns the current version (either new_ or old_)
   * set on this RecordAccessor.
   *
   * @return See above.
   */
  const TRecord &current() const;

 private:
  // The database accessor for which this record accessor is created
  // Provides means of getting to the transaction and database functions.
  // Immutable, set in the constructor and never changed.
  GraphDbAccessor *db_accessor_;

  // The record (edge or vertex) this accessor provides access to.
  // Immutable, set in the constructor and never changed.
  mvcc::VersionList<TRecord> *vlist_;

  /**
   * Latest version which is visible to the current transaction+command
   * but has not been created nor modified by the current transaction+command.
   *
   * Can be null only when the record itself (the version-list) has
   * been created by the current transaction+command.
   */
  TRecord *old_{nullptr};

  /**
   * Version that has been modified (created or updated) by the current
   * transaction+command.
   *
   * Can be null when the record has not been modified in the current
   * transaction+command. It is also possible that the modification
   * has happened, but this RecordAccessor does not know this. To
   * ensure correctness, the `SwitchNew` function must check if this
   * is null, and if it is it must check with the vlist_ if there is
   * an update.
   */
  TRecord *new_{nullptr};

  /**
   * Pointer to the version (either old_ or new_) that READ operations
   * in the accessor should take data from. Note that WRITE operations
   * should always use new_.
   *
   * This pointer can never ever be null.
   */
  TRecord *current_{nullptr};
};
