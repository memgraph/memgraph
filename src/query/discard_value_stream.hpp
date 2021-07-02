#pragma once

#include <vector>

#include "query/typed_value.hpp"

namespace query {
struct DiscardValueResultStream {
  void Result(const std::vector<query::TypedValue> & /*values*/) {
    // do nothing
  }
};
}  // namespace query
