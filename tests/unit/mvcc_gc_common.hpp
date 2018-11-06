#pragma once

#include "storage/single_node/mvcc/record.hpp"
#include "transactions/single_node/engine.hpp"

/**
 * @brief - Empty class which inherits from mvcc:Record.
 */
class Prop : public mvcc::Record<Prop> {
 public:
  Prop *CloneData() { return new Prop; }
};

/**
 * @brief - Class which inherits from mvcc::Record and takes an atomic variable
 * to count number of destructor calls (to test if the record is actually
 * deleted).
 */
class DestrCountRec : public mvcc::Record<DestrCountRec> {
 public:
  explicit DestrCountRec(std::atomic<int> &count) : count_(count) {}
  DestrCountRec *CloneData() { return new DestrCountRec(count_); }
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
    return engine.GlobalGcSnapshot();
  }
}
