#include <iostream>

#include "pool.hpp"

int main(void) {
  auto size = 7;
  auto N = 1000000;

  Pool pool(size);

  for (int i = 0; i < N; ++i)
    pool.run(
        [](int) {
          int sum = 0;

          for (int i = 0; i < 2000; ++i) sum += i % 7;
        },
        i);

  return 0;
}
