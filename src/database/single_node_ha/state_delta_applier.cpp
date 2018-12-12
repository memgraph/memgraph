#include "database/single_node_ha/state_delta_applier.hpp"

#include "database/single_node_ha/graph_db_accessor.hpp"
#include "utils/exceptions.hpp"

namespace database {

StateDeltaApplier::StateDeltaApplier(GraphDb *db) : db_(db) {}

void StateDeltaApplier::Apply(const std::vector<StateDelta> &deltas) {
  for (auto &delta : deltas) {
    switch (delta.type) {
      case StateDelta::Type::TRANSACTION_BEGIN:
        Begin(delta.transaction_id);
        break;
      case StateDelta::Type::TRANSACTION_COMMIT:
        Commit(delta.transaction_id);
        break;
      case StateDelta::Type::TRANSACTION_ABORT:
        LOG(FATAL) << "StateDeltaApplier shouldn't know about aborted "
                      "transactions";
        break;
      case StateDelta::Type::BUILD_INDEX:
      case StateDelta::Type::DROP_INDEX:
        throw utils::NotYetImplemented(
            "High availability doesn't support index at the moment!");
      default:
        delta.Apply(*GetAccessor(delta.transaction_id));
    }
  }
}

void StateDeltaApplier::Begin(const tx::TransactionId &tx_id) {
  CHECK(accessors_.find(tx_id) == accessors_.end())
      << "Double transaction start";
  accessors_.emplace(tx_id, db_->Access());
}

void StateDeltaApplier::Abort(const tx::TransactionId &tx_id) {
  GetAccessor(tx_id)->Abort();
  accessors_.erase(accessors_.find(tx_id));
}

void StateDeltaApplier::Commit(const tx::TransactionId &tx_id) {
  GetAccessor(tx_id)->Commit();
  accessors_.erase(accessors_.find(tx_id));
}

GraphDbAccessor *StateDeltaApplier::GetAccessor(
    const tx::TransactionId &tx_id) {
  auto found = accessors_.find(tx_id);
  CHECK(found != accessors_.end())
      << "Accessor does not exist for transaction: " << tx_id;
  return found->second.get();
}

}  // namespace database
