#pragma once

#include "import/element_skeleton.hpp"
#include "utils/option.hpp"

// Common class for varius classes which accept one part from data line in
// import, parses it and adds it into element skelleton.
class Filler {
 public:
  // Fills skeleton with data from str. Returns error description if
  // error occurs.
  virtual Option<std::string> fill(ElementSkeleton &data, char *str) = 0;
};
