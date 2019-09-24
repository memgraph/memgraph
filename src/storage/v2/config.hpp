#pragma once

#include <chrono>

namespace storage {

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    enum class Type { NONE, PERIODIC };

    Type type{Type::PERIODIC};
    std::chrono::milliseconds interval{std::chrono::milliseconds(1000)};
  } gc;

  struct Items {
    bool properties_on_edges{true};
  } items;
};

}  // namespace storage
