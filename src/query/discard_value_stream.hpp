#pragma once

#include <vector>

#include "query/typed_value.hpp"

namespace query {
struct DiscardValueResultStream final {
  void Result(const std::vector<query::TypedValue> & /*values*/) {
    // do nothing
  }
};
}  // namespace query
