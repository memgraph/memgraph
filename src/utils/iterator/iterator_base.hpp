#pragma once

#include "utils/iterator/count.hpp"
#include "utils/option.hpp"

// Base iterator for next() kind iterator.
// T - type of return value
template <class T>
class IteratorBase {
 public:
  virtual ~IteratorBase(){};

  virtual Option<T> next() = 0;

  virtual Count count() = 0;
};
