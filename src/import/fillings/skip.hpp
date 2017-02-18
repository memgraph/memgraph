#pragma once

#include "import/fillings/common.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"

// Skips column.
class SkipFiller : public Filler {
 public:
  // Fills skeleton with data from str. Returns error description if
  // error occurs.
  Option<std::string> fill(ElementSkeleton &data, char *str) final {
    return make_option<std::string>();
  }
};
