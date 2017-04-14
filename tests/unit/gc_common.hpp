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
class PropCount : public mvcc::Record<PropCount> {
 public:
  PropCount(std::atomic<int> &count) : count_(count) {}
  ~PropCount() { ++count_; }

 private:
  std::atomic<int> &count_;
};
