#pragma once

#include <unordered_set>

#include "utils/option.hpp"
#include "utils/option_ptr.hpp"

#include "storage/model/properties/property_family.hpp"
#include "storage/model/properties/stored_property.hpp"

template <class TG, class T>
using type_key_t =
    typename PropertyFamily<TG>::PropertyType::template PropertyTypeKey<T>;

template <class TG>
class Properties
{
public:
    using property_key =
        typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey;

    template <class T>
    using type_key_t =
        typename PropertyFamily<TG>::PropertyType::template PropertyTypeKey<T>;

    auto begin() const { return props.begin(); }
    auto cbegin() const { return props.cbegin(); }

    auto end() const { return props.end(); }
    auto cend() const { return props.cend(); }

    size_t size() const { return props.size(); }

    const StoredProperty<TG> &at(PropertyFamily<TG> &key) const;

    const StoredProperty<TG> &at(property_key &key) const;

    template <class T>
    OptionPtr<const T> at(type_key_t<T> &key) const
    {
        auto f_key = key.family_key();
        for (auto &prop : props) {
            if (prop.key == f_key) {
                return OptionPtr<const T>(&(prop.template as<T>()));
            }
        }

        return OptionPtr<const T>();
    }

    void set(StoredProperty<TG> &&value);

    void clear(property_key &key);

    void clear(PropertyFamily<TG> &key);

    template <class Handler>
    void accept(Handler &handler) const
    {
        for (auto &kv : props)
            kv.accept(handler);

        handler.finish();
    }

    template <class Handler>
    void handle(Handler &handler) const
    {
        for (auto &kv : props)
            handler.handle(kv);

        handler.finish();
    }

    template <class Handler>
    void for_all(Handler handler) const
    {
        for (auto &kv : props)
            handler(kv);
    }

private:
    using props_t = std::vector<StoredProperty<TG>>;
    // TODO: more bytes can be saved if this is array with exact size as number
    // of elements.
    // TODO: even more bytes can be saved if this is one ptr to structure which
    // holds len followed by len sized array.
    props_t props;
};
