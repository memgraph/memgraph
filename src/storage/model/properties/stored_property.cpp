#include "storage/model/properties/stored_property.hpp"

template <class TG>
const PropertyFamily<TG> StoredProperty<TG>::null_family("NULL");

template <class TG>
const StoredProperty<TG> StoredProperty<TG>::null = {
    Null(), StoredProperty<TG>::null_family.getNull().family_key()};

template class StoredProperty<TypeGroupVertex>;
template class StoredProperty<TypeGroupEdge>;
