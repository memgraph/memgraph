#pragma once

#include "mvcc/version_list.hpp"
#include "storage/typed_value.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "utils/pass_key.hpp"

template<typename TRecord, typename TDerived>
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

  RecordAccessor(mvcc::VersionList<TRecord>& vlist,
                 GraphDbAccessor& db_accessor)
      : vlist_(vlist), record_(vlist_.find(db_accessor.transaction_)), db_accessor_(db_accessor) {
    assert(record_ != nullptr);
  }

  RecordAccessor(mvcc::VersionList<TRecord>& vlist,
                 TRecord& record,
                 GraphDbAccessor& db_accessor)
      : vlist_(vlist), record_(&record), db_accessor_(db_accessor) {
    assert(record_ != nullptr);
  }

  template<typename TValue>
  void PropsSet(GraphDb::Property key, TValue value) {
    update().props_.set(key, value);
  }

  size_t PropsErase(GraphDb::Property key) {
    return update().props_.erase(key);
  }

  const TypedValueStore<GraphDb::Property> &Properties() const {
    return view().properties_;
  }

  void PropertiesAccept(std::function<void(const GraphDb::Property key, const TypedValue &prop)> handler,
                        std::function<void()> finish = {}) const {
    view().props_.Accept(handler, finish);
  }

  // Assumes same transaction
  friend bool operator==(const RecordAccessor &a, const RecordAccessor &b) {
    // TODO consider the legitimacy of this comparison
    return a.vlist_ == b.vlist_;
  }

  // Assumes same transaction
  friend bool operator!=(const RecordAccessor &a, const RecordAccessor &b) {
    // TODO consider the legitimacy of this comparison
    return !(a == b);
  }

  /**
   * Returns a GraphDB accessor of this record accessor.
   *
   * @return See above.
   */
  GraphDbAccessor& db_accessor() {
    return db_accessor_;
  }

  /**
   * Returns a const GraphDB accessor of this record accessor.
   *
   * @return See above.
   */
  const GraphDbAccessor& db_accessor() const {
    return db_accessor_;
  }

protected:

  /**
   * Returns the update-ready version of the record.
   *
   * @return See above.
   */
  TRecord& update() {
    // TODO consider renaming this to something more indicative
    // of the underlying MVCC functionality (like "new_version" or so)
    if (!record_->is_visible_write(db_accessor_.transaction_))
      record_ = vlist_.update(record_, db_accessor_.transaction_);

    return *record_;
  }

  /**
   * Returns a version of the record that is only for viewing.
   *
   * @return See above.
   */
  const TRecord& view() const {
    return *record_;
  }

  // The record (edge or vertex) this accessor provides access to.
  // Immutable, set in the constructor and never changed.
  mvcc::VersionList<TRecord>& vlist_;

  // The database accessor for which this record accessor is created
  // Provides means of getting to the transaction and database functions.
  // Immutable, set in the constructor and never changed.
  GraphDbAccessor& db_accessor_;

private:
  /* The version of the record currently used in this transaction. Defaults to the
   * latest viewable version (set in the constructor). After the first update done
   * through this accessor a new, editable version, is created for this transaction,
   * and set as the value of this variable.
   *
   * Stored as a pointer due to it's mutability (the update() function changes it).
   */
  TRecord* record_;
};
