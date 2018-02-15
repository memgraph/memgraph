#include <algorithm>
#include <mutex>

#include "glog/logging.h"

#include "transactions/engine.hpp"
#include "transactions/tx_end_listener.hpp"

namespace tx {

void Engine::Register(TxEndListener *listener) {
  std::lock_guard<SpinLock> guard{end_listeners_lock_};
  end_listeners_.emplace_back(listener);
}

void Engine::Unregister(TxEndListener *listener) {
  std::lock_guard<SpinLock> guard{end_listeners_lock_};
  auto found =
      std::find(end_listeners_.begin(), end_listeners_.end(), listener);
  CHECK(found != end_listeners_.end())
      << "Failed to find listener to unregister";
  end_listeners_.erase(found);
}

void Engine::NotifyListeners(transaction_id_t tx_id) const {
  std::lock_guard<SpinLock> guard{end_listeners_lock_};
  for (auto *listener : end_listeners_) listener->operator()(tx_id);
}
}  // namespace tx
