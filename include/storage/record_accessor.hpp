#pragma once

#include "mvcc/version_list.hpp"
#include "transactions/transaction.hpp"
#include "storage/typed_value.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "utils/pass_key.hpp"

template <typename TRecord, typename TDerived>
class RecordAccessor {

public:

  RecordAccessor(mvcc::VersionList<TRecord>* vlist, tx::Transaction &trans)
      : vlist_(vlist), trans_(trans) {
    record_ = vlist->find(trans_);
  }

  /**
   * Indicates if this record is visible to the current transaction.
   *
   * @return
   */
  bool is_visible() const {
    return record_ != nullptr;
  }

  TypedValue at(GraphDb::Property key) const {
    return record_->props_.at(key);
  }

  template <typename TValue>
  void set(GraphDb::Property key, TValue value) {
    update();
    record_->props_.set(key, value);
  }

  size_t erase(GraphDb::Property key) {
    update();
    return record_->props_.erase(key);
  }

  void Accept(std::function<void(const GraphDb::Property key, const TypedValue& prop)> handler,
              std::function<void()> finish = {}) const {
    record_->props_.Accept(handler, finish);
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
   * Exposes the version list only to the GraphDb.
   *
   * @param pass_key Ignored.
   * @return The version list of this accessor.
   */
  mvcc::VersionList<TRecord>* vlist(PassKey<GraphDbAccessor> pass_key) const {
    return vlist_;
  }

protected:

  /**
   * Ensures this accessor is fit for updating functions.
   *
   * IMPORTANT:  This function should be called from any
   * method that will change the record (in terms of the
   * property graph data).
   */
  void update() {
    // TODO consider renaming this to something more indicative
    // of the underlying MVCC functionality (like "new_version" or so)
    if (record_->is_visible_write(trans_))
      return;
    else
      record_ = vlist_->update(trans_);
  }

  mvcc::VersionList<TRecord>* vlist_;
  tx::Transaction& trans_;
  TRecord* record_;
};
