#ifndef JSON_ARRAY_HPP
#define JSON_ARRAY_HPP

#include <cassert>
#include <memory>
#include <vector>

#include "../utilities/utils.hpp"

#include "json.hpp"

namespace json
{

class Array final : public Json
{
public:
    Array() {}

    template <typename It>
    Array(It first, It last)
        : elements(first, last) {}

    Array(std::initializer_list<spJson> elements)
        : elements(elements) {}

    virtual bool is_array() const;

    size_t size() const;

    Array& push(const std::shared_ptr<Json>& element);

    const spJson& operator[](size_t i) const;
          spJson  operator[](size_t i);

    virtual operator std::string() const;

private:
    std::vector<spJson> elements;
};

bool Array::is_array() const
{
    return true;
}

size_t Array::size() const
{
    return elements.size();
}

Array& Array::push(const spJson& element)
{
    assert(element);

    elements.push_back(element);
    return *this;
}

const spJson& Array::operator[](size_t i) const
{
    return elements[i];
}

spJson Array::operator[](size_t i)
{
    return elements[i];
}

Array::operator std::string() const
{
    std::vector<std::string> xs;

    std::transform(elements.begin(), elements.end(), std::back_inserter(xs),
        [](const spJson& element) {
            return static_cast<std::string>(*element);
        });

    return "[" + utils::intercalate(xs.begin(), xs.end(), ",") + "]";
}

}

#endif  
