#pragma once

#include <cassert>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_holder.hpp"

class Null;

// Property Class designated for creation outside the database.
class Property : public PropertyHolder<Type>
{
public:
    using PropertyHolder<Type>::PropertyHolder;

    static Property handle(Void &&v);

    static Property handle(bool &&prop);

    static Property handle(float &&prop);

    static Property handle(double &&prop);

    static Property handle(int32_t &&prop);

    static Property handle(int64_t &&prop);

    static Property handle(std::string &&value);

    static Property handle(ArrayStore<bool> &&);

    static Property handle(ArrayStore<int32_t> &&);

    static Property handle(ArrayStore<int64_t> &&);

    static Property handle(ArrayStore<float> &&);

    static Property handle(ArrayStore<double> &&);

    static Property handle(ArrayStore<std::string> &&);
};

using properties_t = std::vector<Property>;
