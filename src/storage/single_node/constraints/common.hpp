/// @file

#pragma once

#include "storage/common/types/property_value_store.hpp"
#include "transactions/single_node/engine.hpp"
#include "transactions/snapshot.hpp"

namespace storage::constraints::common {
template <typename TConstraints>
void UniqueConstraintRefresh(const tx::Snapshot &snapshot,
                             const tx::Engine &engine,
                             TConstraints &constraints, std::mutex &lock) {
  std::lock_guard<std::mutex> guard(lock);
  for (auto &constraint : constraints) {
    for (auto p = constraint.version_pairs.begin();
         p != constraint.version_pairs.end(); ++p) {
      auto exp_id = p->record.tx_id_exp;
      auto cre_id = p->record.tx_id_cre;
      if ((exp_id != 0 && exp_id < snapshot.back() &&
           engine.Info(exp_id).is_committed() && !snapshot.contains(exp_id)) ||
          (cre_id < snapshot.back() && engine.Info(cre_id).is_aborted())) {
        constraint.version_pairs.erase(p);
      }
    }
  }
}
}  // namespace storage::constraints::common
