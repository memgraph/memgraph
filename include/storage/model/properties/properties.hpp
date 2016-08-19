#pragma once

#include <map>

#include "storage/model/properties/property.hpp"
#include "storage/model/properties/property_family.hpp"
#include "utils/option.hpp"

using prop_key_t = PropertyFamily::PropertyType::PropertyFamilyKey;

template <class T>
using type_key_t = PropertyFamily::PropertyType::PropertyTypeKey<T>;

class Properties
{
public:
    using sptr = std::shared_ptr<Properties>;

    auto begin() const { return props.begin(); }
    auto cbegin() const { return props.cbegin(); }

    auto end() const { return props.end(); }
    auto cend() const { return props.cend(); }

    size_t size() const { return props.size(); }

    const Property &at(PropertyFamily &key) const;

    const Property &at(prop_key_t &key) const;

    template <class T>
    auto at(type_key_t<T> &key) const;

    template <class T, class... Args>
    void set(type_key_t<T> &key, Args &&... args);

    void set(prop_key_t &key, Property::sptr value);

    void clear(prop_key_t &key);

    void clear(PropertyFamily &key);

    template <class Handler>
    void accept(Handler &handler) const
    {
        for (auto &kv : props)
            handler.handle(kv.first, *kv.second);

        handler.finish();
    }

private:
    using props_t = std::map<prop_key_t, Property::sptr>;
    props_t props;
};
