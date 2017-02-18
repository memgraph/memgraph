#include "storage/garbage/garbage.hpp"

void Garbage::dispose(tx::Snapshot<Id> &&snapshot, DeleteSensitive *data) {
  // If this fails it's better to leak memory than to cause read after free.
  gar.begin().push(std::make_pair(snapshot, data));
}

void Garbage::clean() {
  for (auto it = gar.begin(); it != gar.end(); it++) {
    if (it->first.all_finished(engine) && it.remove()) {
      // All transactions who could have seen data are finished and this
      // thread successfull removed item from list.
      it->second->~DeleteSensitive();
    }
  }
}
