#include "utils/numerics/saturate.hpp"

std::size_t num::saturating_add(std::size_t a, std::size_t b) {
  a = a >= size_t_HIGHEST_BIT_SETED ? size_t_HIGHEST_BIT_SETED - 1 : a;
  b = b >= size_t_HIGHEST_BIT_SETED ? size_t_HIGHEST_BIT_SETED - 1 : b;

  return a + b;
}
