#pragma once

#include <unordered_map>

#include "storage/model/properties/property.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

template <class TG>
using prop_key_t = typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey;

template <class TG, class T>
using type_key_t =
    typename PropertyFamily<TG>::PropertyType::template PropertyTypeKey<T>;

template <class TG>
class Properties
{
public:
    using sptr = std::shared_ptr<Properties>;

    using prop_key_t =
        typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey;

    template <class T>
    using type_key_t =
        typename PropertyFamily<TG>::PropertyType::template PropertyTypeKey<T>;

    auto begin() const { return props.begin(); }
    auto cbegin() const { return props.cbegin(); }

    auto end() const { return props.end(); }
    auto cend() const { return props.cend(); }

    size_t size() const { return props.size(); }

    const Property &at(PropertyFamily<TG> &key) const;

    const Property &at(prop_key_t &key) const;

    template <class T>
    OptionPtr<T> at(type_key_t<T> &key) const
    {
        auto f_key = key.family_key();
        auto it = props.find(f_key);

        if (it == props.end() || it->first.prop_type() != key.prop_type())
            return OptionPtr<T>();

        return OptionPtr<T>(&(it->second.get()->template as<T>()));
    }

    template <class T, class... Args>
    void set(type_key_t<T> &key, Args &&... args)
    {
        auto value = std::make_shared<T>(std::forward<Args>(args)...);

        // try to emplace the item
        // TODO: There is uneccesary copying of value here.
        auto result = props.emplace(std::make_pair(key, value));

        if (!result.second) {
            // It is necessary to change key because the types from before and
            // now
            // could be different.
            prop_key_t &key_ref = const_cast<prop_key_t &>(result.first->first);
            key_ref = key;
            result.first->second = std::move(value);
        }
    }

    void set(prop_key_t &key, Property::sptr value);

    void clear(prop_key_t &key);

    void clear(PropertyFamily<TG> &key);

    template <class Handler>
    void accept(Handler &handler) const
    {
        for (auto &kv : props)
            handler.handle(kv.first, *kv.second);

        handler.finish();
    }

    template <class Handler>
    void for_all(Handler handler) const
    {
        for (auto &kv : props)
            handler(kv.first, kv.second);
    }

private:
    using props_t =
        std::unordered_map<prop_key_t, Property::sptr, PropertyHash<TG>>;
    props_t props;
};
