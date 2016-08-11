#pragma once

#include "storage/model/properties/floating.hpp"
#include "storage/model/properties/number.hpp"
#include "storage/model/properties/utils/modulo.hpp"

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
