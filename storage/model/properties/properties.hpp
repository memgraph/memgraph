#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTIES_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_PROPERTIES_HPP

#include <map>

#include "property.hpp"

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
        return props.at(key).get();
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

        buffer.pop_back(); // erase last comma
        buffer += '}';
    }

private:
    props_t props;
};

#endif
