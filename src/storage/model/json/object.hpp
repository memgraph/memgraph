#pragma once

#include <iostream>

#include <functional>
#include <sstream>
#include <cassert>
#include <vector>
#include <map>

#include "utilities/string/intercalate.hpp"

#include "json.hpp"

namespace json {

typedef std::pair<const std::string, spJson> const_kv_pair_t;
typedef std::pair<std::string, spJson> kv_pair_t;

class Object;

typedef std::shared_ptr<Object> spObject;

class Object : public Json
{
public:
    Object() {}

    Object(std::initializer_list<const_kv_pair_t> elements)
        : props(elements) {}

    virtual bool is_object() const;

    const spJson& at(const std::string& key) const;
          spJson  at(const std::string& key);

    const spJson& operator[](const std::string& key) const;
          spJson  operator[](const std::string& key);

    Object& put(const std::string& key, spJson value);

    Object& operator<<(const kv_pair_t& kv_pair);

    virtual operator std::string() const;

protected:
    std::map<std::string, spJson> props;
};

bool Object::is_object() const
{
    return true;
}

const spJson& Object::at(const std::string& key) const
{
    return props.at(key);
}

spJson Object::at(const std::string& key)
{
    return props.at(key);
}

const spJson& Object::operator[](const std::string& key) const
{
    return this->at(key);
}

spJson Object::operator[](const std::string& key)
{
    return this->at(key);
}

Object& Object::put(const std::string& key, spJson value)
{
    assert(value);

    props[key] = value;
    return *this;
}

Object& Object::operator<<(const kv_pair_t& kv_pair)
{
    return this->put(std::get<0>(kv_pair), std::get<1>(kv_pair));
}

Object::operator std::string() const
{
    if(props.empty())
        return "{}";

    std::vector<std::string> xs;

    std::transform(props.begin(), props.end(), std::back_inserter(xs),
        [](const kv_pair_t& kvp) {
            return "\"" + kvp.first + "\":" + static_cast<std::string>(*kvp.second);
        });

    return "{" + utils::intercalate(xs.begin(), xs.end(), ",") + "}";
}

}
