#pragma once

#include "utils/iterator/composable.hpp"
#include "utils/iterator/iterator_base.hpp"

namespace iter {

// Class which Combined two iterators IT1 and IT2.
// Both return values T
// T - type of return value
// IT1 - first iterator type
// IT2 - second iterator type
template <class T, class IT1, class IT2>
class Combined : public IteratorBase<T>,
                 public Composable<T, Combined<T, IT1, IT2>> {
 public:
  Combined() = delete;

  // Combined operation is designed to be used in chained calls which operate
  // on a iterator. Combined will in that usecase receive other iterator by
  // value and std::move is a optimization for it.
  Combined(IT1 &&iter1, IT2 &&iter2)
      : iter1(Option<IT1>(std::move(iter1))),
        iter2(Option<IT2>(std::move(iter2))) {}

  // Return values first from first iterator then from second.
  Option<T> next() final {
    if (iter1.is_present()) {
      auto ret = iter1.get().next();
      if (ret.is_present()) {
        return std::move(ret);
      } else {
        iter1.take();
      }
    }

    return iter2.next();
  }

  Count count() final {
    return iter1.map_or([](auto &it) { return it.count(); }, 0) + iter2.count();
  }

 private:
  Option<IT1> iter1;
  IT2 iter2;
};

template <class IT1, class IT2>
auto make_combined(IT1 &&iter1, IT2 &&iter2) {
  // Compiler cant deduce type T. decltype is here to help with it.
  return Combined<decltype(iter1.next().take()), IT1, IT2>(std::move(iter1),
                                                           std::move(iter2));
}
}
