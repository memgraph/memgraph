#include "storage/common/constraints/record.hpp"

#include "storage/common/constraints/exceptions.hpp"
#include "storage/common/mvcc/exceptions.hpp"
#include "transactions/engine.hpp"
#include "transactions/transaction.hpp"

namespace storage::constraints::impl {
Record::Record(gid::Gid gid, const tx::Transaction &t)
    : curr_gid(gid), tx_id_cre(t.id_) {}

void Record::Insert(gid::Gid gid, const tx::Transaction &t) {
  // Insert
  //   - delete before or in this transaction and not aborted
  //   - insert before and aborted
  // Throw SerializationException
  //   - delted of inserted after this transaction
  // Throw ViolationException
  //   - insert before or in this transaction and not aborted
  //   - delete before and aborted

  t.TakeLock(lock_);
  if (t.id_ < tx_id_cre || (tx_id_exp != 0 && t.id_ < tx_id_exp)) {
    throw SerializationException(
        "Node couldn't be updated due to unique constraint serialization "
        "error!");
  }

  bool has_entry = tx_id_exp == 0;
  bool is_aborted = has_entry ? t.engine_.Info(tx_id_cre).is_aborted()
                              : t.engine_.Info(tx_id_exp).is_aborted();

  if ((has_entry && !is_aborted) || (!has_entry && is_aborted)) {
    throw ViolationException(
        "Node couldn't be updated due to unique constraint violation!");
  }

  curr_gid = gid;
  tx_id_cre = t.id_;
  tx_id_exp = 0;
}

void Record::Remove(gid::Gid gid, const tx::Transaction &t) {
  // Remove
  //   - insert before or in this transaction and not aborted
  //   - remove before and aborted
  // Nothing
  //   - remove before or in this transaction and not aborted
  //   - insert before and aborted
  // Throw SerializationException
  //   - delete or insert after this transaction

  t.TakeLock(lock_);
  DCHECK(gid == curr_gid);
  if (t.id_ < tx_id_cre || (tx_id_exp != 0 && t.id_ < tx_id_exp))
    throw mvcc::SerializationError();

  bool has_entry = tx_id_exp == 0;
  bool is_aborted = has_entry ? t.engine_.Info(tx_id_cre).is_aborted()
                              : t.engine_.Info(tx_id_exp).is_aborted();

  if ((!has_entry && !is_aborted) || (has_entry && is_aborted)) return;

  tx_id_exp = t.id_;
}
}  // namespace storage::constraints::impl
