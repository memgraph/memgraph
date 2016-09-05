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
};

using properties_t = std::vector<Property>;
