#pragma once

#include "storage/model/properties/number.hpp"

template <class Derived>
struct Floating : public Number<Derived>
{
    using Number<Derived>::Number;
};
