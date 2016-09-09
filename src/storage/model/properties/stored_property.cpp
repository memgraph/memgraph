#include "storage/model/properties/property.hpp"
#include "storage/model/properties/stored_property.hpp"

template <class TG>
const PropertyFamily<TG> StoredProperty<TG>::null_family("NULL");

template <class TG>
const StoredProperty<TG> StoredProperty<TG>::null = {
    Null(), StoredProperty<TG>::null_family.getNull().family_key()};

template <class TG>
template <class P>
StoredProperty<TG>::StoredProperty(PropertyFamily<TG> &family, P &&property)
    : StoredProperty(std::move(property),
                     family.get(property.key.get_type()).family_key())
{
}

template class StoredProperty<TypeGroupVertex>;
template class StoredProperty<TypeGroupEdge>;

template StoredProperty<TypeGroupVertex>::StoredProperty(
    PropertyFamily<TypeGroupVertex> &family, Property &&property);

template StoredProperty<TypeGroupEdge>::StoredProperty(
    PropertyFamily<TypeGroupEdge> &family, Property &&property);
