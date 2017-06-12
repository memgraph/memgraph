#pragma once

#include "mvcc/record.hpp"

/**
 * @brief - Empty class which inherits from mvcc:Record.
 */
class Prop : public mvcc::Record<Prop> {};

/**
 * @brief - Class which inherits from mvcc::Record and takes an atomic variable
 * to count number of destructor calls (to test if the record is actually
 * deleted).
 */
class DestrCountRec : public mvcc::Record<DestrCountRec> {
 public:
  DestrCountRec(std::atomic<int> &count) : count_(count) {}
  ~DestrCountRec() { ++count_; }

 private:
  std::atomic<int> &count_;
};

// helper function for creating a GC snapshot
// if given a nullptr it makes a GC snapshot like there
// are no active transactions
auto GcSnapshot(tx::Engine &engine, tx::Transaction *t) {
  if (t != nullptr) {
    tx::Snapshot gc_snap = t->snapshot();
    gc_snap.insert(t->id_);
    return gc_snap;
  } else {
    tx::Snapshot gc_snap;
    gc_snap.insert(engine.Count() + 1);
    return gc_snap;
  }
}
