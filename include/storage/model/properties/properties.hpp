#pragma once

#include <map>

#include "storage/model/properties/property.hpp"

class Properties
{
public:
    using sptr = std::shared_ptr<Properties>;

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
