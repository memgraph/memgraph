#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter {

// Class which maps values returned by I iterator into iterators of type J
// ,which
// return value of type T, with function OP.
// function.
// T - type of return value
// I - iterator type
// J - iterator type returned from OP
// OP - type of mapper function
// TODO: Split into flat operation and map operation.
template <class T, class I, class J, class OP>
class FlatMap : public IteratorBase<T>,
                public Composable<T, FlatMap<T, I, J, OP>> {
 public:
  FlatMap() = delete;

  // FlatMap operation is designed to be used in chained calls which operate
  // on a
  // iterator. FlatMap will in that usecase receive other iterator by value
  // and
  // std::move is a optimization for it.
  FlatMap(I &&iter, OP &&op) : iter(std::move(iter)), op(std::move(op)) {}

  FlatMap(FlatMap &&m)
      : iter(std::move(m.iter)),
        op(std::move(m.op)),
        sub_iter(std::move(m.sub_iter)) {}

  ~FlatMap() final {}

  Option<T> next() final {
    do {
      if (!sub_iter.is_present()) {
        auto item = iter.next();
        if (item.is_present()) {
          sub_iter = Option<J>(op(item.take()));
        } else {
          return Option<T>();
        }
      }

      auto item = sub_iter.get().next();
      if (item.is_present()) {
        return std::move(item);
      } else {
        sub_iter.take();
      }
    } while (true);
  }

  Count count() final {
    // TODO: Correct count, are at least correcter
    return iter.count();
  }

 private:
  I iter;
  Option<J> sub_iter;
  OP op;
};

template <class I, class OP>
auto make_flat_map(I &&iter, OP &&op) {
  // Compiler cant deduce type T and J. decltype is here to help with it.
  return FlatMap<decltype(op(iter.next().take()).next().take()), I,
                 decltype(op(iter.next().take())), OP>(std::move(iter),
                                                       std::move(op));
}
}
