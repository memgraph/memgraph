#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter {

// Class which inspects values returned by I iterator
// before passing them as a result.
// function.
// T - type of return value
// I - iterator type
// OP - type of inspector function. OP: T&->void
template <class T, class I, class OP>
class Inspect : public IteratorBase<T>,
                public Composable<T, Inspect<T, I, OP>> {
 public:
  Inspect() = delete;

  // Inspect operation is designed to be used
  // in chained calls which operate on an
  // iterator. Inspect will in that usecase
  // receive other iterator by value and
  // std::move is a optimization for it.
  Inspect(I &&iter, OP &&op) : iter(std::move(iter)), op(std::move(op)) {}

  Inspect(Inspect &&m) : iter(std::move(m.iter)), op(std::move(m.op)) {}

  ~Inspect() final {}

  Option<T> next() final {
    auto item = iter.next();
    if (item.is_present()) {
      op(item.get());
      return std::move(item);
    } else {
      return Option<T>();
    }
  }

  Count count() final { return iter.count(); }

 private:
  I iter;
  OP op;
};

template <class I, class OP>
auto make_inspect(I &&iter, OP &&op) {
  // Compiler cant deduce type T. decltype is here to help with it.
  return Inspect<decltype(iter.next().take()), I, OP>(std::move(iter),
                                                      std::move(op));
}
}
