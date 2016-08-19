#include "storage/model/properties/properties.hpp"

#include "storage/model/properties/null.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

const Property &Properties::at(prop_key_t &key) const
{
    auto it = props.find(key);

    if (it == props.end()) return Property::Null;

    return *it->second.get();
}

template <class T>
auto Properties::at(type_key_t<T> &key) const
{
    auto f_key = key.family_key();
    auto it = props.find(f_key);

    if (it == props.end() || it->first.prop_type() != key.prop_type())
        return Option<decltype(
            &(it->second.get()->template as<T>().value_ref()))>();

    return make_option(&(it->second.get()->template as<T>().value_ref()));
}

template <class T, class... Args>
void Properties::set(type_key_t<T> &key, Args &&... args)
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

void Properties::set(prop_key_t &key, Property::sptr value)
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

void Properties::clear(prop_key_t &key) { props.erase(key); }

// template <class Handler>
// void Properties::accept(Handler& handler) const
// {
//     for(auto& kv : props)
//         handler.handle(kv.first, *kv.second);
//
//     handler.finish();
// }

template <>
inline void Properties::set<Null>(type_key_t<Null> &key)
{
    auto fk = key.family_key();
    clear(fk);
}
