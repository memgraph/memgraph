#pragma once

#include <map>

#include "storage/model/properties/property.hpp"

class Properties
{
public:
    using sptr = std::shared_ptr<Properties>;

    auto begin()  const { return props.begin(); }
    auto cbegin() const { return props.cbegin(); }

    auto end()  const { return props.end(); }
    auto cend() const { return props.cend(); }

    size_t size() const
    {
        return props.size();
    }

    const Property& at(const std::string& key) const;

    template <class T, class... Args>
    void set(const std::string& key, Args&&... args);

    void set(const std::string& key, Property::sptr value);

    void clear(const std::string& key);

    template <class Handler>
    void accept(Handler& handler) const
    {
        for(auto& kv : props)
            handler.handle(kv.first, *kv.second);

        handler.finish();
    }

private:
    using props_t = std::map<std::string, Property::sptr>;
    props_t props;
};
