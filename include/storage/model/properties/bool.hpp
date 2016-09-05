#pragma once

#include "storage/model/properties/flags.hpp"

class Bool
{
public:
    const static Type type;

    Bool(bool d) : data(d) {}

    bool &value() { return data; }

    bool const &value() const { return data; }

    std::ostream &print(std::ostream &stream) const
    {
        return operator<<(stream, *this);
    }

    friend std::ostream &operator<<(std::ostream &stream, const Bool &prop)
    {
        return stream << prop.data;
    }

    bool operator==(const Bool &other) const
    {
        return other.value() == value();
    }

    bool operator==(bool v) const { return value() == v; }

    explicit operator bool() const { return value(); }

private:
    bool data;
};
