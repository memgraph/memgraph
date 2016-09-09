#pragma once

#include "storage/model/properties/flags.hpp"
#include "storage/model/properties/property_family.hpp"
#include "storage/model/properties/property_holder.hpp"

template <class TG>
using property_key =
    typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey;

// Property Class designated for creation outside the database.
template <class TG>
class StoredProperty : public PropertyHolder<property_key<TG>>
{
    // null property family
    const static class PropertyFamily<TG> null_family;

public:
    // Needed for properties to return reference on stored property when they
    // don't cointain searched property.
    const static class StoredProperty<TG> null;

    using PropertyHolder<property_key<TG>>::PropertyHolder;

    template <class P>
    StoredProperty(PropertyFamily<TG> &family, P &&property);
};
