#pragma once

#include <stdint.h>
#include <limits>
#include <ostream>

#include "utils/total_ordering.hpp"

class Id : public TotalOrdering<Id> {
 public:
  Id() = default;

  Id(uint64_t id);

  friend bool operator<(const Id &a, const Id &b);

  friend bool operator==(const Id &a, const Id &b);

  friend std::ostream &operator<<(std::ostream &stream, const Id &id);

  operator uint64_t() const;

  /**
   * @brief - Return maximal possible Id.
   */
  static const Id MaximalId() {
    return Id(std::numeric_limits<uint64_t>::max());
  }

 private:
  uint64_t id{0};
};
