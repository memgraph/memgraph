#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter {

// Class which wraps iterator and hides it's type. It actualy does this by
// dynamicly allocating iterator on heap.
// T - type of return value
template <class T>
class Virtual : public Composable<T, Virtual<T>> {
 public:
  Virtual() = delete;

  // Virtual operation is designed to be used in chained calls which operate
  // on a
  // iterator. Virtual will in that usecase receive other iterator by value
  // and
  // std::move is a optimization for it.

  template <class I>
  Virtual(I &&iter) : it(std::make_unique<I>(std::move(iter))) {}

  Virtual(Virtual &&m) : it(std::move(m.it)) {}

  ~Virtual() {}

  Option<T> next() { return it.get()->next(); }

  Count count() { return it.get()->count(); }

 private:
  std::unique_ptr<IteratorBase<T>> it;
};

template <class I>
auto make_virtual(I &&iter) {
  // Compiler can't deduce type T. decltype is here to help with it.
  return Virtual<decltype(iter.next().take())>(std::move(iter));
}
}
