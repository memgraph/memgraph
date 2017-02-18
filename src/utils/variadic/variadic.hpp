#pragma once

#include <iostream>

namespace utils {

/*
 * Variadic argument print
 */
template <class Head>
void print_vargs(std::ostream& s, Head&& head) {
  s << std::forward<Head>(head);
}
template <class Head, class... Tail>
void print_vargs(std::ostream& s, Head&& head, Tail&&... tail) {
  s << std::forward<Head>(head);
  print_vargs(s, std::forward<Tail>(tail)...);
}

/*
 * Compile time print line.
 *
 * USAGE:
 *     RUN: utils::printer("ONE ", "TWO");
 *     OUTPUT: "ONE TWO\n"
 *
 * TODO: reimplament with C++17 fold expressions
 */
template <class... Args>
void println(Args&&... args) {
  print_vargs(std::cout, std::forward<Args>(args)...);
  std::cout << std::endl;
}

// value equality with any of variadic argument
// example: value == varg[0] OR value == varg[1] OR ...

template <class Value, class Head>
bool _or_vargs(Value&& value, Head&& head) {
  return value == head;
}
template <class Value, class Head, class... Tail>
bool _or_vargs(Value&& value, Head&& head, Tail&&... tail) {
  return value == head || _or_vargs(std::forward<Value>(value), tail...);
}
template <class Value, class... Array>
bool or_vargs(Value&& value, Array&&... array) {
  return _or_vargs(std::forward<Value>(value), std::forward<Array>(array)...);
}
}
