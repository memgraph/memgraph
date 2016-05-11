#pragma once

#include "number.hpp"

template <class Derived>
struct Floating : public Number<Derived>
{
    using Number<Derived>::Number;
};
