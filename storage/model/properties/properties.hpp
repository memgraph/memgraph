#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTIES_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTIES_HPP

#include <map>

#include "property.hpp"
#include "string.hpp"

namespace model
{

class Properties
{
    using props_t = std::map<std::string, Property::sptr>;

public:
    props_t::iterator find(const std::string& key)
    {
        return props.find(key);
    }
    
    Property* at(const std::string& key)
    {
        auto it = props.find(key);
        return it == props.end() ? nullptr : it->second.get();
    }

    void put(const std::string& key, Property::sptr value)
    {
        props[key] = std::move(value);
    }

    void clear(const std::string& key)
    {
        props.erase(key);
    }

    void dump(std::string& buffer)
    {
        buffer += '{';

        for(auto& kvp : props)
        {
            buffer += '"'; buffer += kvp.first; buffer += "\":";
            kvp.second->dump(buffer); buffer += ',';
        }
    
        // replace last redundant comma with }
        buffer.back() = '}';
    }

private:
    props_t props;
};

}

#endif
