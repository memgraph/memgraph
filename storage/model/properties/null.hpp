#pragma once

#include "property.hpp"

class Null : public Property
{
public:
    friend class Property;

    static constexpr Flags type = Flags::Null;

    Null(const Null&) = delete;
    Null(Null&&) = delete;

    Null operator=(const Null&) = delete;

    bool operator==(const Property& other) const override
    {
        return other.is<Null>();
    }

    bool operator==(const Null&) const
    {
        return true;
    }

    explicit operator bool()
    {
        return false;
    }

    friend std::ostream& operator<<(std::ostream& stream, const Null&)
    {
        return stream << "NULL";
    }

    std::ostream& print(std::ostream& stream) const override
    {
        return operator<<(stream, *this);
    }

private:
    // the constructor for null is private, it can be constructed only as a
    // value inside the Property class, Property::Null
    Null() : Property(Flags::Null) {}
};

// Null is a const singleton declared in class Property
// it can be used as a type by using Null or as a value by using Property::Null
const Null Property::Null;

