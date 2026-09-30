#include <usearch/index_plugins.hpp>

int main() {
  using namespace unum::usearch;

  auto metric = metric_kind_t::cos_k;
  auto scalar = scalar_kind_t::f32_k;

  return bits_per_scalar(scalar) == 32 && metric == metric_kind_t::cos_k ? 0 : 1;
}
