#pragma once

#include "database/db_transaction.hpp"
#include "mvcc/version_list.hpp"
#include "transactions/transaction.hpp"
#include "storage/typed_value.hpp"
#include "database/graph_db.hpp"
#include "utils/pass_key.hpp"

template <typename TRecord, typename TDerived>
class RecordAccessor {

public:

  RecordAccessor(mvcc::VersionList<TRecord>* vlist, DbTransaction &db_trans)
      : vlist_(vlist), db_trans_(db_trans) {
    assert(vlist_ != nullptr);
  }

  RecordAccessor(TRecord *t, mvcc::VersionList<TRecord> *vlist, DbTransaction &db_trans)
      : record_(t), vlist_(vlist), db_trans_(db_trans) {
    assert(record_ != nullptr);
    assert(vlist_ != nullptr);
  }

  // TODO: Test this
  TDerived update() const {
    assert(!empty());

    if (record_->is_visible_write(db_trans_.trans)) {
      // TODO: VALIDATE THIS BRANCH. THEN ONLY THIS TRANSACTION CAN SEE
      // THIS DATA WHICH MEANS THAT IT CAN CHANGE IT.
      return TDerived(record_, vlist_, db_trans_);

    } else {
      auto new_record_ = vlist_->update(db_trans_.trans);

      // TODO: Validate that update of record in this accessor is correct.
      const_cast<RecordAccessor *>(this)->record_ = new_record;
      return TDerived(new_record, vlist_, db_trans_);
    }
  }

  TypedValue at(GraphDb::Property key) const {
    return record_->props_.at(key);
  }

  template <typename TValue>
  void set(GraphDb::Property key, TValue value) {
    // TODO should update be called here?!?!
    record_->props_.set(key, value);
  }

  size_t erase(GraphDb::Property key) const {
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
  mvcc::VersionList<TRecord>* vlist(Passkey<GraphDb> pass_key) {
    return vlist_;
  }


protected:
  TRecord* record_{nullptr};
  mvcc::VersionList<TRecord>* vlist_;
  DbTransaction& db_trans_;
};
