#pragma once

template <class T>
class SimpleCounter {
 public:
  SimpleCounter(T initial) : counter(initial) {}

  T next() { return ++counter; }

  T count() { return counter; }

 private:
  T counter;
};
