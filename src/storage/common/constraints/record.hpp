/// @file

#pragma once

#include "storage/common/locking/record_lock.hpp"
#include "storage/gid.hpp"
#include "transactions/type.hpp"

namespace tx {
class Transaction;
}  // namespace tx

namespace storage::constraints::impl {
/// Contains records of creation and deletion of entry in a constraint.
struct Record {
  Record(gid::Gid gid, const tx::Transaction &t);
  void Insert(gid::Gid gid, const tx::Transaction &t);
  void Remove(gid::Gid gid, const tx::Transaction &t);

  gid::Gid curr_gid;
  tx::TransactionId tx_id_cre;
  tx::TransactionId tx_id_exp{0};
  RecordLock lock_;
};
}  // namespace storage::constraints::impl
