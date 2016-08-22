#pragma once

#include "import/element_skeleton.hpp"
#include "utils/option.hpp"

class Filler
{
public:
    // Fills skeleton with data from str. Returns error description if
    // error occurs.
    virtual Option<std::string> fill(ElementSkeleton &data, char *str) = 0;
};
