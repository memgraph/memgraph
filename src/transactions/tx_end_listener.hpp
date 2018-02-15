#pragma once

#include <functional>

#include "transactions/engine.hpp"
#include "transactions/type.hpp"

namespace tx {

/**
 * Wrapper around the given function that registers itself with the given
 * transaction engine. Ensures that the function gets called when a transaction
 * has ended in the engine.
 *
 * Also ensures that the listener gets unregistered from the engine upon
 * destruction. Intended usage is: just create a TxEndListener and ensure it
 * gets destructed before the function it wraps becomes invalid.
 *
 * There are no guarantees that the listener will be called only once for the
 * given transaction id.
 */
class TxEndListener {
 public:
  TxEndListener(Engine &engine, std::function<void(transaction_id_t)> function)
      : engine_(engine), function_(std::move(function)) {
    engine_.Register(this);
  }

  ~TxEndListener() { engine_.Unregister(this); }

  void operator()(transaction_id_t tx_id) const { function_(tx_id); }

 private:
  Engine &engine_;
  std::function<void(transaction_id_t)> function_;
};
}  // namespace tx
