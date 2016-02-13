#pragma once

#include "number.hpp"

template <class Derived>
struct Integral : public Number<Derived>
{
    using Number<Derived>::Number;
};
