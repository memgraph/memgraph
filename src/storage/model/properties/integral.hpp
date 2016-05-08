#pragma once

#include "number.hpp"
#include "floating.hpp"
#include "utils/modulo.hpp"

template <class Derived>
struct Integral : public Number<Derived>, public Modulo<Derived>
{
    using Number<Derived>::Number;

    template <class T, typename = std::enable_if_t<
                std::is_base_of<Floating<T>, T>::value>>
    operator T() const
    {
        return T(this->derived().value);
    }
};
