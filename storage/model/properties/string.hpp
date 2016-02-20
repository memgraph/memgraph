#pragma once

#include "property.hpp"

class String : public Property
{
public:
    static constexpr Flags type = Flags::String;

    String(const std::string& value) : Property(Flags::String), value(value) {}

    String(std::string&& value) : Property(Flags::String), value(value) {}

    String(const String&) = default;
    String(String&&) = default;

    operator const std::string&() const
    {
        return value;
    }

    bool operator==(const Property& other) const override
    {
        return other.is<String>() && operator==(other.as<String>());
    }

    bool operator==(const String& other) const
    {
        return value == other.value;
    }

    bool operator==(const std::string& other) const
    {
        return value == other;
    }

    friend std::ostream& operator<<(std::ostream& stream, const String& prop)
    {
        return stream << prop.value;
    }

    std::ostream& print(std::ostream& stream) const override
    {
        return operator<<(stream, *this);
    }

    std::string value;
};
