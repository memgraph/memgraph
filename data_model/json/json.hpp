#ifndef JSON_JSON_HPP
#define JSON_JSON_HPP

#include <initializer_list>
#include <string>
#include <memory>

namespace json {

class Json;

typedef std::shared_ptr<Json> spJson;

class Json
{
public:
    Json() {}
    virtual ~Json() {}

    virtual bool is_object()   const { return false; }
    virtual bool is_array()    const { return false; }
    virtual bool is_real()     const { return false; }
    virtual bool is_integral() const { return false; }
    virtual bool is_boolean()  const { return false; }
    virtual bool is_null()     const { return false; }

    template <typename T> T& as();
    template <typename T> const T& as() const;

    virtual operator std::string() const = 0;
};

template <typename T>
T& Json::as()
{
    return *dynamic_cast<T*>(this);
}

template <typename T>
const T& Json::as() const
{
    return *dynamic_cast<T*>(this);
}

}

#endif
