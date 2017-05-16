#include "transactions/snapshot.hpp"

#include "transactions/engine.hpp"

template <class id_t>
bool tx::Snapshot<id_t>::all_finished(Engine &engine) const {
  for (auto &sid : active) {
    if (engine.clog.is_active(sid)) {
      return false;
    }
  }

  return true;
}

template class tx::Snapshot<Id>;
