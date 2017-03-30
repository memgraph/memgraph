#pragma once

#include "database/graph_db.hpp"
//#include "database/graph_db_accessor.hpp"
#include "mvcc/version_list.hpp"
#include "storage/property_value.hpp"
#include "utils/pass_key.hpp"

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
  RecordAccessor(mvcc::VersionList<TRecord>& vlist,
                 GraphDbAccessor& db_accessor);

  /**
   * @param vlist MVCC record that this accessor wraps.
   * @param record MVCC version (that is viewable from this
   * db_accessor.transaction)
   *  of the given record. Slightly more optimal then the constructor that does
   * not
   *  accept an already found record.
   * @param db_accessor The DB accessor that "owns" this record accessor.
   */
  RecordAccessor(mvcc::VersionList<TRecord>& vlist, TRecord& record,
                 GraphDbAccessor& db_accessor);

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
  const PropertyValue& PropsAt(GraphDbTypes::Property key) const;

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
  const PropertyValueStore<GraphDbTypes::Property>& Properties() const;

  void PropertiesAccept(std::function<void(const GraphDbTypes::Property key,
                                           const PropertyValue& prop)>
                            handler,
                        std::function<void()> finish = {}) const;

  /**
   * This should be used with care as it's comparing vlist_ pointer records and
   * not actual values inside RecordAccessors.
   */
  friend bool operator<(const RecordAccessor& a, const RecordAccessor& b) {
    debug_assert(a.db_accessor_ == b.db_accessor_,
                 "Not in the same transaction.");  // assume the same
                                                   // db_accessor / transaction
    return a.vlist_ < b.vlist_;
  }

  friend bool operator==(const RecordAccessor& a, const RecordAccessor& b) {
    debug_assert(a.db_accessor_ == b.db_accessor_,
                 "Not in the same transaction.");  // assume the same
                                                   // db_accessor / transaction
    return a.vlist_ == b.vlist_;
  }

  friend bool operator!=(const RecordAccessor& a, const RecordAccessor& b) {
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
  GraphDbAccessor& db_accessor() const;

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
  const uint64_t temporary_id() const;

 protected:
  /**
   * Returns the update-ready version of the record.
   *
   * @return See above.
   */
  TRecord& update();

  /**
   * Returns a version of the record that is only for viewing.
   *
   * @return See above.
   */
  const TRecord& view() const;


 private:

  // The database accessor for which this record accessor is created
  // Provides means of getting to the transaction and database functions.
  // Immutable, set in the constructor and never changed.
  GraphDbAccessor *db_accessor_;

  // The record (edge or vertex) this accessor provides access to.
  // Immutable, set in the constructor and never changed.
  mvcc::VersionList<TRecord> *vlist_;

  /* The version of the record currently used in this transaction. Defaults to
   * the
   * latest viewable version (set in the constructor). After the first update
   * done
   * through this accessor a new, editable version, is created for this
   * transaction,
   * and set as the value of this variable.
   *
   * Stored as a pointer due to it's mutability (the update() function changes
   * it).
   */
  TRecord* record_;
};
