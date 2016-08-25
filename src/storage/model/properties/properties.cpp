#include "storage/model/properties/properties.hpp"

#include "storage/model/properties/null.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class TG>
const Property &Properties<TG>::at(PropertyFamily<TG> &fam) const
{
    // It doesn't matter whit which type
    // find is called, thats why getNull
    // method is here to give fast access
    // to such key.
    auto key = fam.getNull().family_key();
    auto it = props.find(key);

    if (it == props.end()) return Property::Null;

    return *it->second.get();
}

template <class TG>
const Property &Properties<TG>::at(prop_key_t &key) const
{
    auto it = props.find(key);

    if (it == props.end() || it->first.prop_type() != key.prop_type())
        return Property::Null;

    return *it->second.get();
}

template <class TG>
template <class T>
auto Properties<TG>::at(type_key_t<T> &key) const
{
    auto f_key = key.family_key();
    auto it = props.find(f_key);

    if (it == props.end() || it->first.prop_type() != key.prop_type())
        return Option<decltype(
            &(it->second.get()->template as<T>().value_ref()))>();

    return make_option(&(it->second.get()->template as<T>().value_ref()));
}

template <class TG>
template <class T, class... Args>
void Properties<TG>::set(type_key_t<T> &key, Args &&... args)
{
    auto value = std::make_shared<T>(std::forward<Args>(args)...);

    // try to emplace the item
    // TODO: There is uneccesary copying of value here.
    auto result = props.emplace(std::make_pair(key, value));

    if (!result.second) {
        // It is necessary to change key because the types from before and now
        // could be different.
        prop_key_t &key_ref = const_cast<prop_key_t &>(result.first->first);
        key_ref = key;
        result.first->second = std::move(value);
    }
}

template <class TG>
void Properties<TG>::set(prop_key_t &key, Property::sptr value)
{
    // TODO: There is uneccesary copying of value here.
    auto result = props.insert(make_pair(key, value));
    if (!result.second) {
        // It is necessary to change key because the types from before and now
        // could be different.
        prop_key_t &key_ref = const_cast<prop_key_t &>(result.first->first);
        key_ref = key;
        result.first->second = std::move(value);
    }
}

template <class TG>
void Properties<TG>::clear(prop_key_t &key)
{
    props.erase(key);
}

template <class TG>
void Properties<TG>::clear(PropertyFamily<TG> &fam)
{
    // It doesn't matter whit which type
    // find is called, thats why getNull
    // method is here to give fast access
    // to such key.
    auto key = fam.getNull().family_key();
    props.erase(key);
}

template class Properties<TypeGroupEdge>;

template class Properties<TypeGroupVertex>;
