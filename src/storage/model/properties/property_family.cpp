#include "storage/model/properties/property_family.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class T>
PropertyFamily<T>::PropertyFamily(std::string const &name_v)
    : name_v(std::forward<const std::string>(name_v))
{
    null_type = &get(Flags::Null);
}

template <class T>
PropertyFamily<T>::PropertyFamily(std::string &&name_v)
    : name_v(std::move(name_v))
{
    null_type = &get(Flags::Null);
}

template <class T>
std::string const &PropertyFamily<T>::name() const
{
    return name_v;
}

// Returns type if it exists otherwise creates it.
template <class T>
typename PropertyFamily<T>::PropertyType &PropertyFamily<T>::get(Type type)
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

template <class T>
PropertyFamily<T>::PropertyType::PropertyType(PropertyFamily &family, Type type)
    : family(family), type(std::move(type))
{
}

template <class T>
bool PropertyFamily<T>::PropertyType::is(Type &t) const
{
    return type == t;
}

// Returns key ordered on POINTERS to PropertyFamily
template <class T>
typename PropertyFamily<T>::PropertyType::PropertyFamilyKey
PropertyFamily<T>::PropertyType::family_key()
{
    return PropertyFamilyKey(*this);
}

template class PropertyFamily<TypeGroupEdge>;
template class PropertyFamily<TypeGroupVertex>;
