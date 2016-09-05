#include "storage/model/properties/properties.hpp"

#include "storage/model/properties/null.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class TG>
const StoredProperty<TG> &Properties<TG>::at(PropertyFamily<TG> &fam) const
{
    // It doesn't matter whit which type
    // compare is done, thats why getNull
    // method is here to give fast access
    // to such key.
    auto f_key = fam.getNull().family_key();
    return at(f_key);
}

template <class TG>
const StoredProperty<TG> &Properties<TG>::at(property_key &key) const
{
    for (auto &prop : props) {
        if (prop.key == key) {
            return prop;
        }
    }
    return StoredProperty<TG>::null;
}

template <class TG>
void Properties<TG>::set(StoredProperty<TG> &&value)
{
    for (auto &prop : props) {
        if (prop.key == value.key) {
            // It is necessary to change key because the types from before and
            // now
            // could be different.
            StoredProperty<TG> &sp = const_cast<StoredProperty<TG> &>(prop);
            sp = std::move(value);
            return;
        }
    }
    props.push_back(std::move(value));
}

template <class TG>
void Properties<TG>::clear(property_key &key)
{
    auto end = props.end();
    for (auto it = props.begin(); it != end; it++) {
        if (it->key == key) {
            props.erase(it);
            return;
        }
    }
}

template <class TG>
void Properties<TG>::clear(PropertyFamily<TG> &fam)
{
    // It doesn't matter whit which type
    // find is called, thats why getNull
    // method is here to give fast access
    // to such key.
    auto f_key = fam.getNull().family_key();
    clear(f_key);
}

template class Properties<TypeGroupEdge>;

template class Properties<TypeGroupVertex>;
