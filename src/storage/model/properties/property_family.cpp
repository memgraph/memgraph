#include "storage/model/properties/property_family.hpp"

PropertyFamily::PropertyFamily(std::string const &name_v)
    : name_v(std::forward<const std::string>(name_v))
{
    null_type = &get(Flags::Null);
}
PropertyFamily::PropertyFamily(std::string &&name_v) : name_v(std::move(name_v))
{
    null_type = &get(Flags::Null);
}

std::string const &PropertyFamily::name() const { return name_v; }

// Returns type if it exists otherwise creates it.
PropertyFamily::PropertyType &PropertyFamily::get(Type type)
{
    auto acc = types.access();
    auto it = acc.find(type);
    if (it == acc.end()) {
        auto value =
            std::unique_ptr<PropertyType>(new PropertyType(*this, type));
        auto res = acc.insert(type, std::move(value));
        it = res.first;
    }
    return *(it->second);
}

PropertyFamily::PropertyType::PropertyType(PropertyFamily &family, Type type)
    : family(family), type(std::move(type))
{
}

bool PropertyFamily::PropertyType::is(Type &t) const { return type == t; }

// Returns key ordered on POINTERS to PropertyFamily
PropertyFamily::PropertyType::PropertyFamilyKey
PropertyFamily::PropertyType::family_key()
{
    return PropertyFamilyKey(*this);
}
